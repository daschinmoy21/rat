from typing import Any, TypedDict


class Event(TypedDict):
    event_id: str
    source: str
    entity_id: str
    ts_ms: int
    payload: dict[str, Any]
