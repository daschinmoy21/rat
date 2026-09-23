import json
import logging
import os
import time
from pathlib import Path

log = logging.getLogger("rat")

# While the broker is known-down, skip the network and spool straight away —
# one row burning ~60s of Kafka timeouts would stall a full poll for minutes.
_broker_down_until = 0.0
_BROKER_BACKOFF_S = 60


def _broker_unavailable(err) -> bool:
    from kafka.errors import (
        BrokerNotAvailableError,
        KafkaConnectionError,
        KafkaTimeoutError,
        MetadataEmptyBrokerList,
        NodeNotReadyError,
    )
    return isinstance(err, (
        KafkaTimeoutError,
        KafkaConnectionError,
        NodeNotReadyError,
        BrokerNotAvailableError,
        MetadataEmptyBrokerList,
    ))


def serialize(envelope: dict) -> bytes:
    return json.dumps(envelope, sort_keys=True).encode("utf-8")


def record_key(envelope: dict) -> bytes:
    entities = envelope.get("entities") or []
    key = entities[0] if entities else envelope["event_id"]
    return key.encode("utf-8")


def _spool_dir():
    return Path(os.environ.get(
        "RAT_DLQ_SPOOL", Path.home() / ".local/state/rat/dlq-spool"))


def _spool(envelope, reason, last):
    log.error("dlq send failed: %s — spooling locally", last)
    try:
        spool = _spool_dir()
        spool.mkdir(parents=True, exist_ok=True)
        row = json.dumps({"error": str(reason), "dlq_error": str(last),
                          "envelope": envelope}, default=str)
        with open(spool / "dlq.jsonl", "a") as f:
            f.write(row + "\n")
    except Exception as err:  # even the spool is best-effort; never raise
        log.error("spool write failed: %s — row lost: %s", err, str(envelope)[:500])


def _dlq_parts(envelope):
    """(topic, key, value, headers) for any envelope, even a broken one.

    Schema-invalid rows can be None, a string, or missing event_id — the DLQ
    path must still carry them somewhere instead of raising.
    """
    source = envelope.get("source") if isinstance(envelope, dict) else None
    topic = f"events.{source if isinstance(source, str) and source else 'unknown'}.dlq"
    try:
        key = record_key(envelope)
    except Exception:
        key = b"unknown"
    try:
        value = serialize(envelope)
    except Exception:
        value = json.dumps({"raw": str(envelope)}).encode("utf-8")
    return topic, key, value


def dlq(envelope, producer, reason, max_attempts=3):
    """Last-resort path for bad or undeliverable rows: the source's DLQ topic,
    then a local JSON-lines spool. Never raises."""
    global _broker_down_until
    topic, key, value = _dlq_parts(envelope)
    headers = [("rat.error", str(reason).encode("utf-8"))]
    last = None
    in_backoff = time.time() < _broker_down_until
    if not in_backoff:
        for _ in range(max_attempts):
            try:
                producer.send(topic, key=key, value=value, headers=headers).get(timeout=10)
                return
            except Exception as err:
                last = err
                if _broker_unavailable(err):
                    _broker_down_until = time.time() + _BROKER_BACKOFF_S
                    break
    _spool(envelope, reason, last or "broker in backoff")


def emit(envelope, producer, max_attempts=3):
    global _broker_down_until
    topic = f"events.{envelope['source']}"
    key, value = record_key(envelope), serialize(envelope)
    last = None
    for _ in range(max_attempts):
        if time.time() < _broker_down_until:
            last = last or "broker in backoff"
            break
        try:
            producer.send(topic, key=key, value=value).get(timeout=10)
            return
        except Exception as err:
            last = err
            if _broker_unavailable(err):
                _broker_down_until = time.time() + _BROKER_BACKOFF_S
    dlq(envelope, producer, f"send failed after {max_attempts} attempts: {last}")


def connect(bootstrap=None):
    from kafka import KafkaProducer
    if bootstrap is None:
        bootstrap = os.environ.get("RAT_BOOTSTRAP", "localhost:9092")
    return KafkaProducer(
        bootstrap_servers=bootstrap, acks="all", retries=5, linger_ms=5, max_block_ms=10000
    )

