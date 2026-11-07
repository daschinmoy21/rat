import base64
import json
import socket

import pytest
from fake_iggy import FakeIggy

from rat_producers import cli, iggy, producer
from rat_producers.app import App
from rat_producers.cli import run
from rat_producers.cursor import Cursor
from rat_producers.dash import Feed, consume_iggy
from rat_producers.iggy import (
    IggyClient,
    IggyError,
    IggyProducer,
    IggyUnavailable,
    decode_headers,
    encode_headers,
    send_body,
)
from rat_producers.producer import dlq, emit


def envelope(**overrides):
    env = {"event_id": "hn:1", "source": "hn", "entities": ["AAPL"],
           "ts_ms": 1725200000000, "payload": {"title": "$AAPL does a thing"}}
    env.update(overrides)
    return env


@pytest.fixture()
def broker():
    with FakeIggy() as fake:
        yield fake


@pytest.fixture()
def client(broker):
    c = IggyClient(url=broker.url, timeout=5)
    iggy.provision(c, ["events.hn", "events.hn.dlq", "events.rss", "events.rss.dlq"],
                   partitions=3, env={}, log=lambda *_: None)
    return c


@pytest.fixture(autouse=True)
def _reset_backoff():
    yield
    producer._broker_down_until = 0.0


def _dead_url():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()  # nothing listens here now
    return f"http://127.0.0.1:{port}"


# -- wire encoding -----------------------------------------------------------

def test_send_body_is_base64_with_keyed_partitioning():
    body = send_body(b"AAPL", b'{"a":1}', [("rat.error", b"schema: bad")])
    assert body["partitioning"] == {"kind": "messages_key", "value": "QUFQTA=="}
    msg = body["messages"][0]
    assert msg["id"] == 0
    assert base64.b64decode(msg["payload"]) == b'{"a":1}'
    header = msg["user_headers"][0]
    assert header["key"] == {"kind": "string", "value": base64.b64encode(b"rat.error").decode()}
    assert base64.b64decode(header["value"]["value"]) == b"schema: bad"


def test_send_body_without_headers_omits_them_and_empty_key_is_balanced():
    body = send_body(b"", b"x")
    assert "user_headers" not in body["messages"][0]
    assert body["partitioning"] == {"kind": "balanced", "value": ""}


def test_keys_and_header_values_clip_to_255_bytes_on_char_boundary():
    long = ("é" * 200).encode()  # 400 bytes, 2 per char
    body = send_body(long, b"x", [("rat.error", long)])
    key = base64.b64decode(body["partitioning"]["value"])
    value = base64.b64decode(body["messages"][0]["user_headers"][0]["value"]["value"])
    assert len(key) == 254 and key.decode()  # still valid utf-8
    assert len(value) == 254 and value.decode()


def test_empty_header_value_is_replaced_not_sent_empty():
    encoded = encode_headers([("rat.error", b"")])
    assert base64.b64decode(encoded[0]["value"]["value"]) == b"-"


def test_decode_headers_round_trip_and_tolerates_opaque_string():
    encoded = encode_headers([("rat.error", "boom")])
    assert decode_headers(encoded) == [("rat.error", b"boom")]
    assert decode_headers("AAAA") == []
    assert decode_headers([{"key": {}}]) == []


def test_config_from_env_defaults_and_overrides():
    assert iggy.config_from_env({})["url"] == "http://127.0.0.1:3000"
    cfg = iggy.config_from_env({"RAT_IGGY_URL": "http://h:1/", "RAT_IGGY_STREAM": "s",
                                "RAT_IGGY_PAT": "pat"})
    assert (cfg["url"], cfg["stream"], cfg["token"]) == ("http://h:1", "s", "pat")


def test_backend_selection(monkeypatch):
    monkeypatch.delenv("RAT_BROKER", raising=False)
    assert producer.backend() == "kafka"
    monkeypatch.setenv("RAT_BROKER", "Iggy")
    assert producer.backend() == "iggy"
    assert isinstance(producer.connect(), IggyProducer)
    monkeypatch.setenv("RAT_BROKER", "pulsar")
    with pytest.raises(ValueError):
        producer.backend()


# -- client against the fake HTTP broker ---------------------------------------

def test_ping_and_provision_is_idempotent(broker, client):
    assert client.ping()
    assert client.topics() == ["events.hn", "events.hn.dlq", "events.rss", "events.rss.dlq"]
    assert broker.streams["rat"]["events.hn"]["message_expiry"] == 604800000 * 1000
    assert broker.streams["rat"]["events.hn.dlq"]["message_expiry"] == 2592000000 * 1000
    iggy.provision(client, ["events.hn"], partitions=3, env={}, log=lambda *_: None)
    assert len(broker.streams["rat"]["events.hn"]["partitions"]) == 3
    assert broker.streams["rat"]["events.hn"]["options"]["durability"] == "persisted"


def test_provision_refuses_existing_non_persisted_topic(broker, client):
    broker.streams["rat"]["events.hn"]["options"]["durability"] = "replicated"
    with pytest.raises(IggyError, match="non_persisted_topic"):
        client.ensure_topic("events.hn")


@pytest.mark.parametrize("attestation", ["replicated", "none", ""])
def test_http_201_without_persisted_attestation_is_not_an_ack(broker, client, attestation):
    broker.produce_durability_override = attestation
    with pytest.raises(IggyError, match="not_persisted"):
        client.send("events.hn", b"AAPL", b"{}")


def test_relogin_once_on_expired_token(broker, client):
    client.send("events.hn", b"AAPL", b"{}")
    broker.expire_tokens()
    client.send("events.hn", b"AAPL", b"{}")
    logins = [r for r in broker.requests if r[1] == "/users/login"]
    assert len(logins) == 2
    assert len(broker.messages("rat", "events.hn")) == 2


def test_bad_credentials_raise_not_loop(broker):
    c = IggyClient(url=broker.url, password="wrong")
    with pytest.raises(IggyError) as info:
        c.send("events.hn", b"k", b"v")
    assert info.value.status == 401


def test_send_poll_and_offsets_round_trip(client):
    conf = client.send("events.hn", b"AAPL", b"one")
    pid = conf[0]["partition_id"]
    client.send("events.hn", b"AAPL", b"two")  # same key -> same partition
    polled = client.poll("events.hn", pid, offset=0, count=10)
    assert [iggy.decode_message(m)["value"] for m in polled["messages"]] == [b"one", b"two"]
    assert [m["header"]["offset"] for m in polled["messages"]] == [0, 1]
    assert polled["messages"][0]["header"]["id"] == 2**100  # u128 survives json
    assert client.get_offset("events.hn", pid, "rat-engine") is None
    client.store_offset("events.hn", pid, 0, "rat-engine")
    assert client.get_offset("events.hn", pid, "rat-engine") == 0
    nxt = client.poll("events.hn", pid, kind="next", consumer_id="rat-engine")
    assert [m["header"]["offset"] for m in nxt["messages"]] == [1]


def test_missing_topic_is_404_error(client):
    with pytest.raises(IggyError) as info:
        client.send("events.ghost", b"k", b"v")
    assert info.value.status == 404
    assert not isinstance(info.value, IggyUnavailable)
    assert client.get_topic("events.ghost") is None


def test_unreachable_broker_is_unavailable_status_zero():
    c = IggyClient(url=_dead_url(), timeout=2)
    assert not c.ping()
    with pytest.raises(IggyUnavailable) as info:
        c.send("events.hn", b"k", b"v")
    assert info.value.status == 0
    assert producer._broker_unavailable(info.value)
    assert not producer._broker_unavailable(IggyUnavailable(503, "busy"))


# -- producer policy (emit / dlq / spool) on the iggy backend -----------------

def test_emit_lands_payload_identical_to_kafka_value(broker, client):
    env = envelope()
    emit(env, IggyProducer(client))
    [msg] = broker.messages("rat", "events.hn")
    assert base64.b64decode(msg["payload"]) == producer.serialize(env)


def test_emit_retries_transient_503_then_succeeds(broker, client):
    broker.fail_next = [503, 503]
    emit(envelope(), IggyProducer(client), max_attempts=3)
    assert len(broker.messages("rat", "events.hn")) == 1
    assert broker.messages("rat", "events.hn.dlq") == []
    assert producer._broker_down_until == 0.0  # busy is not down


def test_emit_falls_back_to_dlq_with_error_header(broker, client):
    broker.fail_next = [503, 503, 503]
    emit(envelope(), IggyProducer(client), max_attempts=3)
    assert broker.messages("rat", "events.hn") == []
    [msg] = broker.messages("rat", "events.hn.dlq")
    headers = dict(decode_headers(msg["user_headers"]))
    assert "send failed after 3 attempts" in headers["rat.error"].decode()
    assert json.loads(base64.b64decode(msg["payload"])) == envelope()


def test_unknown_source_without_topics_spools(tmp_path, monkeypatch, client):
    monkeypatch.setenv("RAT_DLQ_SPOOL", str(tmp_path / "spool"))
    emit(envelope(source="ghost"), IggyProducer(client))
    rows = (tmp_path / "spool" / "dlq.jsonl").read_text().splitlines()
    assert json.loads(rows[0])["envelope"]["source"] == "ghost"


def test_broker_down_spools_and_opens_backoff(tmp_path, monkeypatch):
    monkeypatch.setenv("RAT_DLQ_SPOOL", str(tmp_path / "spool"))
    p = IggyProducer(IggyClient(url=_dead_url(), timeout=2))
    emit(envelope(), p)
    assert producer._broker_down_until > 0
    dlq(envelope(), p, "schema: x")  # in backoff: straight to spool
    rows = (tmp_path / "spool" / "dlq.jsonl").read_text().splitlines()
    assert len(rows) == 2


def test_run_loop_over_iggy_moves_cursor_and_dlqs_bad_rows(tmp_path, broker, client):
    rows = [envelope(event_id="hn:bad", ts_ms="soon"),
            envelope(event_id="hn:1", ts_ms=1000), envelope(event_id="hn:2", ts_ms=2000)]
    app = App()
    app.add_source("hn", poll=lambda since: list(rows))
    app.schemas["hn"] = None
    app.manifests["hn"] = {"interval": "60s", "topic": "events.hn"}
    run(app, Cursor(tmp_path / "c.db"), IggyProducer(client), once=True)
    assert len(broker.messages("rat", "events.hn")) == 2
    assert len(broker.messages("rat", "events.hn.dlq")) == 1
    assert Cursor(tmp_path / "c.db").get("hn") == 2000


# -- ops surface ------------------------------------------------------------------

def test_dlq_depths_and_peek(client):
    p = IggyProducer(client)
    for i in range(4):
        dlq(envelope(event_id=f"hn:{i}", entities=[f"E{i}"]), p, f"schema: bad {i}")
    assert iggy.dlq_depths(client, ["events.hn.dlq", "events.ghost.dlq"]) == {
        "events.hn.dlq": 4, "events.ghost.dlq": 0}
    rows = iggy.peek(client, "events.hn.dlq", n=3)
    assert len(rows) == 3
    assert [r["value"]["event_id"] for r in rows] == ["hn:1", "hn:2", "hn:3"]
    assert rows[-1]["error"] == "schema: bad 3"


def _cli_app():
    app = App()
    app.manifests["hn"] = {"topic": "events.hn", "interval": "60s"}
    app.manifests["rss"] = {"topic": "events.rss", "interval": "5m"}
    return app


def _iggy_env(monkeypatch, url, tmp_path):
    monkeypatch.setenv("RAT_BROKER", "iggy")
    monkeypatch.setenv("RAT_IGGY_URL", url)
    monkeypatch.setenv("RAT_CURSOR_DB", str(tmp_path / "c.db"))
    monkeypatch.setenv("RAT_SINK_DIR", str(tmp_path / "sinks"))


def test_cli_topics_status_and_dlq_on_iggy(tmp_path, monkeypatch, capsys, broker):
    _iggy_env(monkeypatch, broker.url, tmp_path)
    monkeypatch.setenv("RAT_PARTITIONS", "2")
    app = _cli_app()
    assert cli.provision_topics(app) == 0
    assert sorted(broker.streams["rat"]) == [
        "events.hn", "events.hn.dlq", "events.rss", "events.rss.dlq"]
    assert len(broker.streams["rat"]["events.rss"]["partitions"]) == 2
    dlq(envelope(), IggyProducer(IggyClient(url=broker.url)), "schema: nope")
    capsys.readouterr()
    cli.print_status(app)
    out = capsys.readouterr().out
    assert "iggy " + broker.url in out and " up" in out
    assert "events.hn.dlq=1" in out and "events.rss.dlq=0" in out

    class Args:
        topic, n = None, 5
    cli.print_dlq(app, Args)
    out = capsys.readouterr().out
    assert "events.hn.dlq  1 messages" in out
    assert "error=schema: nope" in out


def test_cli_status_iggy_down(tmp_path, monkeypatch, capsys):
    _iggy_env(monkeypatch, _dead_url(), tmp_path)
    cli.print_status(_cli_app())
    out = capsys.readouterr().out
    assert "DOWN" in out and "(broker down)" in out


def test_cli_topics_refuses_on_kafka_backend(monkeypatch, capsys):
    monkeypatch.delenv("RAT_BROKER", raising=False)
    assert cli.provision_topics(_cli_app()) == 1
    assert "make-topics.sh" in capsys.readouterr().out


def test_dash_iggy_reader_tails_events_and_dlq(broker, client):
    import threading
    p = IggyProducer(client)
    emit(envelope(), p)
    emit(envelope(event_id="hn:2", entities=["MSFT"]), p)
    dlq(envelope(event_id="hn:3"), p, "schema: bad")
    feed, stop = Feed(cap=100), threading.Event()
    t = threading.Thread(target=consume_iggy, args=(feed, client, stop),
                         kwargs={"idle_s": 0.05}, daemon=True)
    t.start()
    import time
    deadline = time.time() + 5
    while time.time() < deadline and feed.seq < 3:
        time.sleep(0.05)
    stop.set()
    t.join(timeout=5)
    events, dlqs, _ = feed.since(0)
    assert sorted(r["event_id"] for r in events) == ["hn:1", "hn:2"]
    assert dlqs[0]["error"] == "schema: bad"
    assert feed.broker["up"] is True
    assert feed.seq == 3  # no duplicates after repeated polls
