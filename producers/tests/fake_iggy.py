"""In-process stand-in for the Iggy 0.9.0 HTTP API subset rat uses.

Implements docs/iggy-interface.md: JWT login, /ping, streams/topics by name,
POST messages (base64, messages_key partitioning, 201 + confirmations), GET
messages (offset/first/last/next), consumer offsets, error bodies. Partitions
are 0-based. Good enough to drive the real urllib client; not a broker.
"""

import base64
import json
import threading
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse


class FakeIggy:
    def __init__(self, username="iggy", password="iggy"):
        self.username, self.password = username, password
        self.tokens = set()
        self.streams = {}  # name -> {topic name -> {"partitions": [[msg]], ...}}
        self.offsets = {}  # (stream, topic, consumer, partition) -> offset
        self.requests = []
        self.fail_next = []  # statuses to return for the next data requests
        self.produce_durability_override = None
        self.lock = threading.Lock()
        self._ts = 1_700_000_000_000_000
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.02}, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()

    def expire_tokens(self):
        self.tokens.clear()

    def messages(self, stream, topic):
        parts = self.streams[stream][topic]["partitions"]
        return [m for p in parts for m in p]

    # -- handler -------------------------------------------------------------

    def _handler(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _reply(self, status, body=None, text=None, headers=None):
                data = (text.encode() if text is not None
                        else b"" if body is None else json.dumps(body).encode())
                self.send_response(status)
                if data:
                    self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                for key, value in (headers or {}).items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(data)

            def _err(self, status, code, reason="", field=None):
                self._reply(status, {"id": status, "code": code, "reason": reason,
                                     "field": field})

            def _body(self):
                n = int(self.headers.get("Content-Length") or 0)
                return json.loads(self.rfile.read(n) or b"{}")

            def _authed(self):
                auth = self.headers.get("Authorization", "")
                if not auth.startswith("Bearer ") or auth[7:] not in fake.tokens:
                    self._err(401, "unauthenticated", "Unauthenticated")
                    return False
                return True

            def do_GET(self):
                self._route("GET")

            def do_POST(self):
                self._route("POST")

            def do_PUT(self):
                self._route("PUT")

            def _route(self, method):
                url = urlparse(self.path)
                parts = [unquote(p) for p in url.path.strip("/").split("/")]
                query = {k: v[0] for k, v in parse_qs(url.query).items()}
                with fake.lock:
                    fake.requests.append((method, url.path, query))
                if url.path == "/ping":
                    return self._reply(200, text="pong")
                if url.path == "/users/login" and method == "POST":
                    body = self._body()
                    if (body.get("username"), body.get("password")) != (
                            fake.username, fake.password):
                        return self._err(401, "invalid_credentials")
                    token = f"jwt-{len(fake.tokens)}-{id(body)}"
                    fake.tokens.add(token)
                    return self._reply(200, {"user_id": 0, "access_token": {
                        "token": token, "expiry": 9_999_999_999}})
                if not self._authed():
                    return None
                if fake.fail_next and parts[-1:] == ["messages"]:
                    status = fake.fail_next.pop(0)
                    return self._err(status, "injected")
                with fake.lock:
                    return self._dispatch(method, parts, query)

            def _dispatch(self, method, parts, query):
                if parts == ["streams"] and method == "POST":
                    name = self._body()["name"]
                    if name in fake.streams:
                        return self._err(400, "stream_name_already_exists", field="name")
                    fake.streams[name] = {}
                    return self._reply(200, self._stream(name))
                if len(parts) < 2 or parts[0] != "streams" or parts[1] not in fake.streams:
                    return self._err(404, "not_found", "Resource not found")
                stream = parts[1]
                topics = fake.streams[stream]
                if len(parts) == 2:
                    return self._reply(200, self._stream(stream))
                if parts[2:] == ["topics"] and method == "POST":
                    body = self._body()
                    if body["name"] in topics:
                        return self._err(400, "topic_name_already_exists", field="name")
                    topics[body["name"]] = {
                        "partitions": [[] for _ in range(body["partitions_count"])],
                        "message_expiry": body["message_expiry"],
                        "options": body.get("options", {})}
                    return self._reply(200, self._topic(stream, body["name"]))
                if len(parts) < 4 or parts[3] not in topics:
                    return self._err(404, "not_found", "Resource not found")
                topic = parts[3]
                if len(parts) == 4:
                    return self._reply(200, self._topic(stream, topic))
                if parts[4] == "messages" and method == "POST":
                    return self._send(stream, topic, self._body())
                if parts[4] == "messages" and method == "GET":
                    return self._poll(stream, topic, query)
                if parts[4] == "consumer-offsets" and method == "PUT":
                    b = self._body()
                    fake.offsets[(stream, topic, str(b["consumer_id"]),
                                  int(b["partition_id"]))] = int(b["offset"])
                    return self._reply(204)
                if parts[4] == "consumer-offsets" and method == "GET":
                    key = (stream, topic, query["consumer_id"], int(query["partition_id"]))
                    if key not in fake.offsets:
                        return self._err(404, "not_found")
                    part = topics[topic]["partitions"][key[3]]
                    return self._reply(200, {"partition_id": key[3],
                                             "current_offset": max(len(part) - 1, 0),
                                             "stored_offset": fake.offsets[key]})
                return self._err(404, "not_found")

            def _stream(self, name):
                topics = [{"id": i, "name": t, "partitions_count": len(d["partitions"]),
                           "messages_count": sum(len(p) for p in d["partitions"])}
                          for i, (t, d) in enumerate(sorted(fake.streams[name].items()))]
                return {"id": 0, "name": name, "topics_count": len(topics),
                        "messages_count": sum(t["messages_count"] for t in topics),
                        "topics": topics}

            def _topic(self, stream, name):
                d = fake.streams[stream][name]
                parts = [{"id": i, "current_offset": max(len(p) - 1, 0),
                          "messages_count": len(p), "segments_count": 1}
                         for i, p in enumerate(d["partitions"])]
                return {"id": 0, "name": name, "partitions_count": len(parts),
                        "messages_count": sum(p["messages_count"] for p in parts),
                        "message_expiry": d["message_expiry"], "partitions": parts,
                        "options": {k: {"value": v, "explicit": True}
                                    for k, v in d.get("options", {}).items()}}

            def _send(self, stream, topic, body):
                parts = fake.streams[stream][topic]["partitions"]
                kind = body["partitioning"]["kind"]
                value = base64.b64decode(body["partitioning"]["value"])
                if kind == "messages_key":
                    if not 1 <= len(value) <= 255:
                        return self._err(400, "invalid_key_value_length")
                    pid = zlib.crc32(value) % len(parts)
                elif kind == "partition_id":
                    pid = int.from_bytes(value, "little")
                else:
                    pid = 0
                base = len(parts[pid])
                for msg in body["messages"]:
                    base64.b64decode(msg["payload"], validate=True)
                    for h in msg.get("user_headers") or []:
                        for side in ("key", "value"):
                            raw = base64.b64decode(h[side]["value"], validate=True)
                            if not 1 <= len(raw) <= 255:
                                return self._err(400, "invalid_header_value")
                    fake._ts += 1000
                    parts[pid].append({
                        "header": {"checksum": 1, "id": 2**100, "offset": len(parts[pid]),
                                   "timestamp": fake._ts, "origin_timestamp": fake._ts,
                                   "user_headers_length": 0,
                                   "payload_length": len(msg["payload"]), "reserved": 0},
                        "payload": msg["payload"],
                        "user_headers": msg.get("user_headers") or []})
                policy = fake.produce_durability_override
                if policy is None:
                    policy = fake.streams[stream][topic]["options"].get("durability", "replicated")
                return self._reply(201, {"confirmations": [
                    {"stream_id": 0, "topic_id": 0, "partition_id": pid,
                     "base_offset": base}]},
                                   headers={"Iggy-Durability": policy} if policy else {})

            def _poll(self, stream, topic, q):
                parts = fake.streams[stream][topic]["partitions"]
                pid = int(q.get("partition_id", 0))
                if pid >= len(parts):
                    return self._err(400, "partition_not_found", field="partition_id")
                part = parts[pid]
                count = int(q.get("count", 10))
                kind = q.get("kind", "offset")
                if kind == "first":
                    start = 0
                elif kind == "last":
                    start = max(len(part) - count, 0)
                elif kind == "next":
                    stored = fake.offsets.get((stream, topic, q["consumer_id"], pid))
                    start = 0 if stored is None else stored + 1
                else:
                    start = int(q.get("value", 0))
                msgs = part[start:start + count]
                return self._reply(200, {"partition_id": pid,
                                         "current_offset": max(len(part) - 1, 0),
                                         "count": len(msgs), "messages": msgs})

        return Handler
