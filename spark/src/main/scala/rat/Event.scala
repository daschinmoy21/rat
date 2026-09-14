package rat

final case class Event(
    eventId: String,
    source: String,
    entities: List[String],
    tsMs: Long,
    payload: String
)
