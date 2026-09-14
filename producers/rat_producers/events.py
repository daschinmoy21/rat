from typing import Any, TypedDict


class Event(TypedDict):
    event_id: str
    source: str
    entities: list[str]
    ts_ms: int
    payload: dict[str, Any]
