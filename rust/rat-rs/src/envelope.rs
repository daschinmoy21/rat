//! Envelope parsing with the same outcome as the legacy
//! `from_json(value, envelopeSchema)` followed by
//! `filter(event_id IS NOT NULL AND ts_ms IS NOT NULL)`.
//!
//! Spark's JSON reader (PERMISSIVE, the `from_json` default) behaves like this,
//! and the code below copies it:
//! * malformed JSON or a non-object top level gives a null row, so the record is dropped;
//! * a `StringType` field takes a JSON string as-is, and any other non-null
//!   JSON value as its compact JSON text (so an object `payload` is kept as JSON text);
//! * a field whose JSON type cannot convert (e.g. `ts_ms: "123"`, `entities: "AAPL"`)
//!   becomes null while the other fields survive (partial results);
//! * `ts_ms` must be a JSON integer that fits in i64.

use serde_json::Value;

/// A parsed envelope. `event_id` and `ts_ms` are guaranteed present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub event_id: String,
    pub source: Option<String>,
    /// `None` = missing / null / not an array. Elements may be null.
    pub entities: Option<Vec<Option<String>>>,
    pub ts_ms: i64,
    pub payload: Option<String>,
}

/// Why a record never reached either sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reject {
    /// Not valid UTF-8 or not valid JSON.
    Malformed,
    /// Valid JSON but not an object.
    NotObject,
    /// `event_id` missing or null.
    MissingEventId,
    /// `ts_ms` missing, null, or not an i64 integer.
    MissingTs,
}

impl Reject {
    pub fn as_str(self) -> &'static str {
        match self {
            Reject::Malformed => "malformed",
            Reject::NotObject => "not_object",
            Reject::MissingEventId => "missing_event_id",
            Reject::MissingTs => "missing_ts_ms",
        }
    }
}

impl Envelope {
    pub fn parse(bytes: &[u8]) -> Result<Envelope, Reject> {
        let value: Value = serde_json::from_slice(bytes).map_err(|_| Reject::Malformed)?;
        let Value::Object(mut map) = value else {
            return Err(Reject::NotObject);
        };
        let event_id = map
            .get("event_id")
            .and_then(spark_string)
            .ok_or(Reject::MissingEventId)?;
        let ts_ms = map
            .get("ts_ms")
            .and_then(Value::as_i64)
            .ok_or(Reject::MissingTs)?;
        let source = map.get("source").and_then(spark_string);
        let entities = match map.get("entities") {
            Some(Value::Array(items)) => Some(items.iter().map(spark_string).collect()),
            _ => None,
        };
        let payload = map.remove("payload").and_then(|v| spark_string(&v));
        Ok(Envelope {
            event_id,
            source,
            entities,
            ts_ms,
            payload,
        })
    }

    /// The legacy raw-sink predicate is `coalesce(size(entities), 0) === 0`.
    /// This port counts a missing/null entities array as entity-less, which is
    /// what the legacy comment and docs describe. Note that Spark 3.5's default
    /// `size(NULL) = -1` (spark.sql.legacy.sizeOfNull=true) makes the legacy job
    /// drop those rows instead; see docs/rust-port.md.
    pub fn is_entityless(&self) -> bool {
        self.entities.as_ref().is_none_or(Vec::is_empty)
    }

    /// Distinct, non-blank entities in first-seen order (`array_distinct`, then
    /// `explode`, then the null/blank filter). Blank means empty after trimming
    /// ASCII spaces, as Spark `trim` does.
    pub fn join_entities(&self) -> Vec<&str> {
        let mut out: Vec<&str> = Vec::new();
        if let Some(entities) = &self.entities {
            for entity in entities.iter().flatten() {
                if !is_blank(entity) && !out.contains(&entity.as_str()) {
                    out.push(entity);
                }
            }
        }
        out
    }

    /// Source that may take part in a join: present and not blank.
    pub fn join_source(&self) -> Option<&str> {
        self.source.as_deref().filter(|s| !is_blank(s))
    }
}

/// Spark `trim(col) = ''`: only the space character is trimmed.
pub fn is_blank(s: &str) -> bool {
    s.trim_matches(' ').is_empty()
}

fn spark_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<Envelope, Reject> {
        Envelope::parse(s.as_bytes())
    }

    #[test]
    fn object_payload_is_kept_as_compact_json_in_key_order() {
        let e = parse(r#"{"event_id":"hn:1","source":"hn","entities":["AAPL"],"ts_ms":5,"payload":{"z": 1, "a": [1, 2]}}"#).unwrap();
        assert_eq!(e.payload.as_deref(), Some(r#"{"z":1,"a":[1,2]}"#));
        assert_eq!(e.entities, Some(vec![Some("AAPL".into())]));
    }

    #[test]
    fn rejects() {
        assert_eq!(parse("{not json"), Err(Reject::Malformed));
        assert_eq!(parse("[1,2]"), Err(Reject::NotObject));
        assert_eq!(parse(r#"{"ts_ms":1}"#), Err(Reject::MissingEventId));
        assert_eq!(parse(r#"{"event_id":"x"}"#), Err(Reject::MissingTs));
        assert_eq!(
            parse(r#"{"event_id":"x","ts_ms":"12"}"#),
            Err(Reject::MissingTs)
        );
        assert_eq!(
            parse(r#"{"event_id":"x","ts_ms":1.5}"#),
            Err(Reject::MissingTs)
        );
        assert_eq!(
            parse(r#"{"event_id":null,"ts_ms":1}"#),
            Err(Reject::MissingEventId)
        );
    }

    #[test]
    fn partial_results_null_bad_fields_only() {
        let e = parse(r#"{"event_id":7,"ts_ms":1,"entities":"AAPL","source":true}"#).unwrap();
        assert_eq!(e.event_id, "7");
        assert_eq!(e.source.as_deref(), Some("true"));
        assert_eq!(e.entities, None);
        assert!(e.is_entityless());
    }

    #[test]
    fn entity_cleanup() {
        let e = parse(r#"{"event_id":"x","ts_ms":1,"entities":[null,""," ","AAPL","AAPL","\tX"]}"#)
            .unwrap();
        // tab is not trimmed by Spark trim, so "\tX" survives
        assert_eq!(e.join_entities(), vec!["AAPL", "\tX"]);
        assert!(!e.is_entityless());
    }
}
