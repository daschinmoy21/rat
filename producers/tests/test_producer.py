import json

from rat_producers.extract import extract_entities
from rat_producers.producer import emit, record_key, serialize


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
