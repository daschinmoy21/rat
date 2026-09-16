import json
import time

import pytest

from rat_producers import producer
from rat_producers.extract import extract_entities
from rat_producers.producer import dlq, emit, record_key, serialize


class FakeFuture:
    def get(self, timeout=None):
        return None  # delivery always succeeds in tests


class FakeProducer:
    def __init__(self):
        self.sent: list[tuple[str, bytes, bytes, list | None]] = []

    def send(self, topic, key, value, headers=None):
        self.sent.append((topic, key, value, headers))
        return FakeFuture()


class FailingProducer(FakeProducer):
    """send() raises for main topics, succeeds for the DLQ."""

    def send(self, topic, key, value, headers=None):
        if not topic.endswith(".dlq"):
            raise ConnectionError("broker down")
        return super().send(topic, key, value, headers)


def envelope(**overrides):
    env = {
        "event_id": "hn:1",
        "source": "hn",
        "entities": ["AAPL"],
        "ts_ms": 1725200000000,
        "payload": {"title": "$AAPL does a thing"},
    }
    env.update(overrides)
    return env


def test_key_is_first_entity():
    assert record_key(envelope()) == b"AAPL"


def test_key_falls_back_to_event_id():
    env = envelope(entities=[])
    assert record_key(env) == b"hn:1"


def test_serialize_round_trips():
    env = envelope()
    assert json.loads(serialize(env).decode()) == env


def test_serialize_is_deterministic():
    env = envelope(payload={"title": "t", "score": 1})
    assert serialize(env) == serialize(dict(reversed(list(env.items()))))


def test_emit_routes_to_source_topic():
    p = FakeProducer()
    emit(envelope(), p)
    assert p.sent[0][0] == "events.hn"


def test_emit_carries_extractor_output():
    p = FakeProducer()
    env = envelope(
        entities=extract_entities("$aapl and $AAPL and $GOOG"),
        payload={"title": "$aapl and $AAPL and $GOOG"},
    )
    emit(env, p)
    value = json.loads(p.sent[0][2].decode())
    assert value["entities"] == ["AAPL", "GOOG"]


def test_emit_dlq_after_retry_budget():
    p = FailingProducer()
    env = envelope(source="ghost", entities=[])
    emit(env, p, max_attempts=3)
    assert len(p.sent) == 1
    topic, key, value, headers = p.sent[0]
    assert topic == "events.ghost.dlq"
    assert key == b"hn:1"
    assert json.loads(value.decode()) == env
    assert headers[0][0] == "rat.error"
    assert "broker down" in headers[0][1].decode()


class AlwaysFailingProducer(FakeProducer):
    def send(self, topic, key, value, headers=None):
        raise ConnectionError("broker down")


def test_dlq_spools_locally_when_even_dlq_fails(tmp_path, monkeypatch):
    monkeypatch.setenv("RAT_DLQ_SPOOL", str(tmp_path / "spool"))
    env = envelope()
    dlq(env, AlwaysFailingProducer(), "broker down")
    spool = tmp_path / "spool" / "dlq.jsonl"
    rows = [json.loads(line) for line in spool.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["error"] == "broker down"
    assert rows[0]["dlq_error"] == "broker down"
    assert rows[0]["envelope"] == env


def test_emit_spools_after_retry_budget_and_dlq_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("RAT_DLQ_SPOOL", str(tmp_path / "spool"))
    emit(envelope(), AlwaysFailingProducer(), max_attempts=2)
    rows = [json.loads(line) for line in (tmp_path / "spool" / "dlq.jsonl").read_text().splitlines()]
    assert rows[0]["error"] == "send failed after 2 attempts: broker down"


def test_dlq_send_failure_reason_is_descriptive():
    p = FailingProducer()
    emit(envelope(), p, max_attempts=2)
    headers = dict(p.sent[0][3])
    assert "send failed after 2 attempts" in headers["rat.error"].decode()


class NoBrokerProducer(FakeProducer):
    def __init__(self):
        super().__init__()
        self.send_calls = 0

    def send(self, topic, key, value, headers=None):
        from kafka.errors import KafkaConnectionError
        self.send_calls += 1
        raise KafkaConnectionError("no brokers")


@pytest.fixture()
def _reset_backoff():
    yield
    producer._broker_down_until = 0.0


def test_broker_down_spools_fast_without_retry_storm(tmp_path, monkeypatch, _reset_backoff):
    monkeypatch.setenv("RAT_DLQ_SPOOL", str(tmp_path / "spool"))
    p = NoBrokerProducer()
    emit(envelope(), p, max_attempts=3)
    first_calls = p.send_calls

    emit(envelope(), p, max_attempts=3)
    # second emit skipped the network entirely: the backoff window is open
    assert p.send_calls == first_calls

    rows = [json.loads(line) for line in (tmp_path / "spool" / "dlq.jsonl").read_text().splitlines()]
    assert len(rows) == 2


def test_backoff_expires_and_send_resumes(tmp_path, monkeypatch, _reset_backoff):
    monkeypatch.setenv("RAT_DLQ_SPOOL", str(tmp_path / "spool"))
    p = NoBrokerProducer()
    emit(envelope(), p)
    assert p.send_calls == 1

    producer._broker_down_until = time.time() - 1  # window over
    emit(envelope(), p)
    assert p.send_calls == 2


def test_dlq_survives_broken_envelopes(tmp_path, monkeypatch):
    monkeypatch.setenv("RAT_DLQ_SPOOL", str(tmp_path / "spool"))
    p = FakeProducer()
    for broken in (None, "a string", {}, {"source": "hn"}, {"source": 7}):
        dlq(broken, p, "schema: bad")
    topics = [t for t, *_ in p.sent]
    assert topics.count("events.unknown.dlq") == 4  # None, "a string", {}, {"source": 7}
    assert topics.count("events.hn.dlq") == 1
    assert (tmp_path / "spool" / "dlq.jsonl").exists() == False  # all reached the DLQ topic


def test_dlq_spools_when_envelope_is_unserializable(tmp_path, monkeypatch):
    monkeypatch.setenv("RAT_DLQ_SPOOL", str(tmp_path / "spool"))
    dlq({"event_id": "x", "entities": [], "source": "hn", "bad": object()},
        AlwaysFailingProducer(), "schema: bad")
    rows = [json.loads(line) for line in (tmp_path / "spool" / "dlq.jsonl").read_text().splitlines()]
    assert rows[0]["error"] == "schema: bad"


def test_spool_failure_does_not_raise(tmp_path, monkeypatch):
    spool_dir = tmp_path / "spool"
    spool_dir.mkdir()
    (spool_dir / "file-not-a-dir").write_text("occupied")
    monkeypatch.setenv("RAT_DLQ_SPOOL", str(spool_dir / "file-not-a-dir"))
    dlq(envelope(), AlwaysFailingProducer(), "broker down")  # must not raise
