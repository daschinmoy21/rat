"""Apache Iggy backend over its HTTP REST API (stdlib only).

Contract: docs/iggy-interface.md. Kafka topic `events.<source>[.dlq]` maps to
topic of the same name inside stream $RAT_IGGY_STREAM (default `rat`); record
key -> `messages_key` partitioning; headers -> string user headers. Every byte
field travels as standard padded base64.

`IggyProducer` mimics the subset of kafka-python's KafkaProducer that
producer.emit/dlq use (`send(...).get()`, `flush`, `close`) so the retry, DLQ
and spool policy stays in one place.
"""

import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_URL = "http://127.0.0.1:3000"
DEFAULT_STREAM = "rat"
MAX_KEY_BYTES = 255
MAX_HEADER_BYTES = 255
NEVER_EXPIRE = 2**64 - 1
TRANSIENT_STATUS = {408, 425, 429, 500, 502, 503, 504}


class IggyError(Exception):
    """A non-2xx reply. `status`/`code` come from the HTTP status and the
    Iggy error body ({"id", "code", "reason", "field"})."""

    def __init__(self, status, code="", reason="", body=None):
        self.status, self.code, self.reason, self.body = status, code, reason, body
        super().__init__(f"iggy http {status} {code}: {reason}".strip())


class IggyUnavailable(IggyError):
    """Broker unreachable or transiently refusing (connect error, timeout, 5xx).
    producer._broker_unavailable() opens the backoff window on these."""


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def unb64(text: str) -> bytes:
    return base64.b64decode(text)


def _clip(data: bytes, limit: int) -> bytes:
    """At most `limit` bytes without splitting a UTF-8 sequence."""
    if len(data) <= limit:
        return data
    return data[:limit].decode("utf-8", "ignore").encode("utf-8")


def encode_headers(headers):
    """[(name, bytes|str)] -> Iggy user_headers list; None when empty.
    Values are clipped to 255 bytes; empty values (illegal in Iggy) are
    replaced with "-"."""
    out = []
    for name, value in headers or []:
        if isinstance(value, str):
            value = value.encode("utf-8")
        elif value is None:
            value = b""
        key = _clip(str(name).encode("utf-8"), MAX_HEADER_BYTES) or b"-"
        value = _clip(bytes(value), MAX_HEADER_BYTES) or b"-"
        out.append({"key": {"kind": "string", "value": b64(key)},
                    "value": {"kind": "string", "value": b64(value)}})
    return out or None


def decode_headers(user_headers):
    """Iggy user_headers (list, or opaque base64 string) -> [(name, bytes)]."""
    if not isinstance(user_headers, list):
        return []
    out = []
    for entry in user_headers:
        try:
            key = unb64(entry["key"]["value"]).decode("utf-8", "replace")
            value = unb64(entry["value"]["value"])
        except (KeyError, TypeError, ValueError):
            continue
        out.append((key, value))
    return out


def send_body(key: bytes, value: bytes, headers=None) -> dict:
    """JSON body for POST .../messages: one message, keyed partitioning."""
    message = {"id": 0, "payload": b64(value)}
    encoded = encode_headers(headers)
    if encoded:
        message["user_headers"] = encoded
    key = _clip(key or b"", MAX_KEY_BYTES)
    partitioning = ({"kind": "messages_key", "value": b64(key)} if key
                    else {"kind": "balanced", "value": ""})
    return {"partitioning": partitioning, "messages": [message]}


def config_from_env(env=None):
    env = os.environ if env is None else env
    return {
        "url": env.get("RAT_IGGY_URL", DEFAULT_URL).rstrip("/"),
        "username": env.get("RAT_IGGY_USERNAME", "iggy"),
        "password": env.get("RAT_IGGY_PASSWORD", "iggy"),
        # RAT_IGGY_PAT matches the Rust engine; RAT_IGGY_TOKEN kept as an alias
        "token": env.get("RAT_IGGY_PAT") or env.get("RAT_IGGY_TOKEN") or None,
        "stream": env.get("RAT_IGGY_STREAM", DEFAULT_STREAM),
        "timeout": float(env.get("RAT_IGGY_TIMEOUT_S", "10")),
    }


def _q(segment) -> str:
    return urllib.parse.quote(str(segment), safe="")


class IggyClient:
    """Thin HTTP client: login, stream/topic admin, send, poll, offsets.
    Logs in lazily and re-logs in once on a 401 (tokens expire, and the JWT
    secret is random per broker start)."""

    def __init__(self, url=DEFAULT_URL, username="iggy", password="iggy",
                 token=None, stream=DEFAULT_STREAM, timeout=10.0, opener=None):
        self.url = url.rstrip("/")
        self.username, self.password, self.pat = username, password, token
        self.stream = stream
        self.timeout = timeout
        self._open = opener or urllib.request.urlopen
        self._jwt = None
        self._last_durability = None

    @classmethod
    def from_env(cls, env=None, **overrides):
        cfg = config_from_env(env)
        cfg.update(overrides)
        return cls(**cfg)

    # -- transport ---------------------------------------------------------

    def _raw(self, method, path, body=None, auth=True, query=None):
        url = self.url + path
        if query:
            url += "?" + urllib.parse.urlencode(query)
        data = None if body is None else json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if auth:
            req.add_header("Authorization", f"Bearer {self._jwt}")
        try:
            with self._open(req, timeout=self.timeout) as resp:
                status = resp.status
                if method == "POST" and path.endswith("/messages"):
                    self._last_durability = resp.headers.get("Iggy-Durability")
                payload = resp.read()
        except urllib.error.HTTPError as err:
            status = err.code
            payload = err.read() if hasattr(err, "read") else b""
            try:
                info = json.loads(payload or b"{}")
            except ValueError:
                info = {}
            if not isinstance(info, dict):
                info = {}
            cls = IggyUnavailable if status in TRANSIENT_STATUS else IggyError
            raise cls(status, info.get("code", ""), info.get("reason", ""), info) from None
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as err:
            raise IggyUnavailable(0, "unreachable", f"{self.url}: {err}") from None
        if not payload:
            return status, None
        try:
            return status, json.loads(payload)
        except ValueError:
            return status, payload.decode("utf-8", "replace")

    def login(self):
        if self.pat:
            _, info = self._raw("POST", "/personal-access-tokens/login",
                                {"token": self.pat}, auth=False)
        else:
            _, info = self._raw("POST", "/users/login",
                                {"username": self.username, "password": self.password},
                                auth=False)
        token = (info or {}).get("access_token") or {}
        if not token.get("token"):
            raise IggyError(200, "no_token", "login returned no access token", info)
        self._jwt = token["token"]
        return self._jwt

    def request(self, method, path, body=None, query=None):
        if self._jwt is None:
            self.login()
        try:
            return self._raw(method, path, body, query=query)
        except IggyError as err:
            if err.status != 401:
                raise
        self.login()
        return self._raw(method, path, body, query=query)

    # -- health / admin ----------------------------------------------------

    def ping(self) -> bool:
        try:
            _, body = self._raw("GET", "/ping", auth=False)
        except IggyError:
            return False
        return isinstance(body, str) and body.strip() == "pong"

    def _topic_path(self, topic):
        return f"/streams/{_q(self.stream)}/topics/{_q(topic)}"

    def get_stream(self):
        """StreamDetails, or None when the stream does not exist."""
        try:
            return self.request("GET", f"/streams/{_q(self.stream)}")[1]
        except IggyError as err:
            if err.status == 404:
                return None
            raise

    def topics(self) -> list[str]:
        stream = self.get_stream()
        return sorted(t["name"] for t in (stream or {}).get("topics") or [])

    def get_topic(self, topic):
        """TopicDetails, or None when the stream or topic does not exist."""
        try:
            return self.request("GET", self._topic_path(topic))[1]
        except IggyError as err:
            if err.status == 404:
                return None
            raise

    def ensure_stream(self):
        if self.get_stream() is not None:
            return False
        try:
            self.request("POST", "/streams", {"name": self.stream})
        except IggyError as err:
            # lost a race with another provisioner: fine if it exists now
            if err.status != 400 or self.get_stream() is None:
                raise
            return False
        return True

    def ensure_topic(self, topic, partitions=1, expiry_us=0, max_size=0):
        existing = self.get_topic(topic)
        if existing is not None:
            policy = (existing.get("options") or {}).get("durability")
            policy = policy.get("value") if isinstance(policy, dict) else policy
            if policy != "persisted":
                raise IggyError(409, "non_persisted_topic",
                                f"{topic} uses {policy!r} durability; choose a fresh stream "
                                "and provision persisted topics before producing. "
                                "Existing data was not modified.")
            return False
        body = {"name": topic, "partitions_count": int(partitions),
                "compression_algorithm": "none", "message_expiry": int(expiry_us),
                "max_topic_size": int(max_size),
                "options": {"durability": "persisted",
                            "consumer_offset_durability": "persisted"}}
        try:
            self.request("POST", f"/streams/{_q(self.stream)}/topics", body)
        except IggyError as err:
            if err.status != 400 or self.get_topic(topic) is None:
                raise
            # A concurrent provisioner may have chosen a weaker policy.
            return self.ensure_topic(topic, partitions, expiry_us, max_size)
        return True

    # -- data plane ----------------------------------------------------------

    def send(self, topic, key: bytes, value: bytes, headers=None):
        """Require HTTP 201 and the broker's persisted-durability attestation."""
        self._last_durability = None
        status, body = self.request("POST", self._topic_path(topic) + "/messages",
                                    send_body(key, value, headers))
        if status != 201:
            raise IggyError(status, "not_committed", f"unexpected produce status {status}")
        if self._last_durability != "persisted":
            raise IggyError(409, "not_persisted",
                            f"produce attested {self._last_durability!r}, expected persisted; "
                            "HTTP 201 alone does not guarantee disk persistence")
        return (body or {}).get("confirmations", []) if isinstance(body, dict) else []

    def poll(self, topic, partition_id, offset=0, count=100, kind="offset",
             consumer_id="rat-cli"):
        """PolledMessages dict for one partition (auto_commit always off)."""
        query = {"consumer_id": consumer_id, "partition_id": int(partition_id),
                 "kind": kind, "value": int(offset), "count": int(count),
                 "auto_commit": "false"}
        return self.request("GET", self._topic_path(topic) + "/messages", query=query)[1]

    def store_offset(self, topic, partition_id, offset, consumer_id):
        self.request("PUT", self._topic_path(topic) + "/consumer-offsets",
                     {"consumer_id": consumer_id, "partition_id": int(partition_id),
                      "offset": int(offset)})

    def get_offset(self, topic, partition_id, consumer_id):
        """Stored (inclusive) offset, or None when never stored."""
        try:
            _, body = self.request("GET", self._topic_path(topic) + "/consumer-offsets",
                                   query={"consumer_id": consumer_id,
                                          "partition_id": int(partition_id)})
        except IggyError as err:
            if err.status == 404:
                return None
            raise
        return body["stored_offset"]


def decode_message(msg) -> dict:
    """One polled message -> {offset, timestamp_s, value(bytes), headers}."""
    header = msg.get("header") or {}
    ts_us = header.get("timestamp") or header.get("origin_timestamp")
    try:
        value = unb64(msg.get("payload") or "")
    except ValueError:
        value = b""
    return {
        "offset": header.get("offset"),
        "timestamp_s": ts_us / 1_000_000 if ts_us else None,
        "value": value,
        "headers": decode_headers(msg.get("user_headers")),
    }


class _Done:
    def get(self, timeout=None):
        return None


class IggyProducer:
    """KafkaProducer-shaped adapter: send() is synchronous (POST, 201 = ack),
    so .get() is already resolved and errors surface from send() itself,
    inside emit()'s retry loop."""

    def __init__(self, client: IggyClient):
        self.client = client

    def send(self, topic, key=None, value=None, headers=None):
        self.client.send(topic, key or b"", value or b"", headers)
        return _Done()

    def flush(self, timeout=None):
        return None

    def close(self, timeout=None):
        return None


def connect(env=None) -> IggyProducer:
    return IggyProducer(IggyClient.from_env(env))


# -- ops surface (status / dlq / provisioning) -------------------------------

def dlq_depths(client: IggyClient, topics):
    """Messages retained in each DLQ topic (0 when the topic is missing)."""
    depths = {}
    for topic in topics:
        details = client.get_topic(topic)
        depths[topic] = int((details or {}).get("messages_count") or 0)
    return depths


def peek(client: IggyClient, topic, n=5):
    """The last n messages of a topic, across partitions, newest last."""
    details = client.get_topic(topic)
    rows = []
    for part in (details or {}).get("partitions") or []:
        if not part.get("messages_count"):
            continue
        polled = client.poll(topic, part["id"], offset=0, count=n, kind="last") or {}
        for msg in polled.get("messages") or []:
            m = decode_message(msg)
            error = None
            for name, val in m["headers"]:
                if name == "rat.error":
                    error = val.decode("utf-8", "replace")
            try:
                value = json.loads(m["value"].decode("utf-8", "replace"))
            except ValueError:
                value = {"raw": repr(m["value"])[:500]}
            rows.append({"topic": topic, "partition": part["id"], "offset": m["offset"],
                         "timestamp_s": m["timestamp_s"], "error": error, "value": value})
    rows.sort(key=lambda row: (row["timestamp_s"] or 0, row["offset"] or 0))
    return rows[-n:]


def plugin_topics(manifests):
    """Event + DLQ topic per manifest, sorted, deduplicated."""
    topics = set()
    for m in manifests:
        t = m.get("topic")
        if t:
            topics.update({t, f"{t}.dlq"})
    return sorted(topics)


def provision(client: IggyClient, topics, partitions=None, env=None, log=print):
    """Create the stream and every topic if missing. Idempotent."""
    env = os.environ if env is None else env
    partitions = int(partitions or env.get("RAT_PARTITIONS", "6"))
    events_us = int(env.get("RAT_EVENTS_RETENTION_MS", "604800000")) * 1000
    dlq_us = int(env.get("RAT_DLQ_RETENTION_MS", "2592000000")) * 1000
    created = client.ensure_stream()
    log(f"stream {client.stream}: {'created' if created else 'exists'}")
    for topic in topics:
        expiry = dlq_us if topic.endswith(".dlq") else events_us
        made = client.ensure_topic(topic, partitions=partitions, expiry_us=expiry)
        log(f"topic  {topic}: {'created' if made else 'exists'} "
            f"(partitions={partitions}, expiry_us={expiry})")


def wait_ready(client: IggyClient, timeout_s=120, every_s=1.0, sleep=time.sleep):
    deadline = time.monotonic() + timeout_s
    while True:
        if client.ping():
            return True
        if time.monotonic() >= deadline:
            return False
        sleep(every_s)
