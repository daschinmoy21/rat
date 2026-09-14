package rat

final case class Event(
    eventId: String,
    source: String,
    entityId: String,
    tsMs: Long,
    payload: String
)
