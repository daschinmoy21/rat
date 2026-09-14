import json


def serialize(envelope: dict) -> bytes:
    return json.dumps(envelope, sort_keys=True).encode("utf-8")


def record_key(envelope: dict) -> bytes:
    entities = envelope.get("entities") or []
    key = entities[0] if entities else envelope["event_id"]
    return key.encode("utf-8")

def emit(envelope,producer,max_attempts=3):
    topic = f"events.{envelope['source']}"
    key,value = record_key(envelope),serialize(envelope)
    last = None
    for _ in range(max_attempts):
        try:
            producer.send(topic,key=key,value=value).get(timeout=10)
            return 
        except Exception as err:
            last = err 
    producer.send(
        f"{topic}.dlq", key=key, value=value,
        headers=[("rat.error", str(last).encode("utf-8"))],
    ).get(timeout=10)

def connect(bootstrap="localhost:9092"):
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers = bootstrap,acks="all",retries=5,linger_ms=5,max_block_ms=10000
    )
