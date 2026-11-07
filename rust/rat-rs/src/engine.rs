//! Event-time correlation engine, the port of `Correlate.pairs` plus the
//! raw-sink branch.
//!
//! The output depends only on the order of input records, never on how they
//! were split into micro-batches or where a restart happened:
//!
//! * The watermark is `max(ts_ms of earlier join-eligible records) - window`.
//!   It moves forward one record at a time, not once per micro-batch as Spark does.
//! * A join-eligible record whose `ts_ms` is strictly below the watermark is
//!   late. It is counted and kept out of the join (Spark drops late rows from
//!   stream-stream joins too). Raw rows are never dropped as late, because the
//!   legacy raw sink has no watermark.
//! * Two records pair when they share an entity, their sources differ, and
//!   `|ts_a - ts_b| <= window` (inclusive both ways). `a` is the record with the
//!   lexicographically smaller source (byte order, same as Spark's UTF8String).
//!   The pair is emitted once, when its second record arrives.
//! * State older than `watermark - window` is evicted. It can never pair with
//!   a non-late record, so eviction timing does not change output.
//!
//! `LatePolicy::Keep` turns off late-dropping and eviction. The result then
//! equals the legacy batch join (what `CorrelateSpec` tests), at the cost of
//! unbounded state.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::envelope::Envelope;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LatePolicy {
    Drop,
    Keep,
}

impl std::str::FromStr for LatePolicy {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "drop" => Ok(LatePolicy::Drop),
            "keep" => Ok(LatePolicy::Keep),
            other => Err(format!("late policy must be drop|keep, got {other:?}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRow {
    pub event_id: String,
    pub source: Option<String>,
    pub entities: Option<Vec<Option<String>>>,
    pub ts_ms: i64,
    pub payload: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PairRow {
    pub entity: String,
    pub a_id: String,
    pub b_id: String,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Entry {
    ts: i64,
    source: String,
    event_id: String,
}

/// Per-record outcome counters, persisted with the checkpoint.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Counters {
    pub accepted: u64,
    pub raw_rows: u64,
    pub correlated_rows: u64,
    /// Join-eligible records dropped because they were behind the watermark.
    pub late_dropped: u64,
    /// Records with entities but no usable source or entity: in neither sink
    /// (the legacy job drops them the same way).
    pub unjoinable: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EngineState {
    max_ts: Option<i64>,
    entries: HashMap<String, Vec<Entry>>,
    pub counters: Counters,
}

pub struct Engine {
    window_ms: i64,
    policy: LatePolicy,
    state: EngineState,
}

#[derive(Default)]
pub struct Output {
    pub raw: Vec<RawRow>,
    pub pairs: Vec<PairRow>,
}

impl Engine {
    pub fn new(window_ms: i64, policy: LatePolicy, state: EngineState) -> Engine {
        Engine {
            window_ms,
            policy,
            state,
        }
    }

    pub fn state(&self) -> &EngineState {
        &self.state
    }

    pub fn watermark(&self) -> Option<i64> {
        self.state.max_ts.map(|m| m.saturating_sub(self.window_ms))
    }

    pub fn state_entries(&self) -> usize {
        self.state.entries.values().map(Vec::len).sum()
    }

    pub fn process(&mut self, env: Envelope, out: &mut Output) {
        self.state.counters.accepted += 1;
        let joinable = match env.join_source() {
            Some(source) => {
                let entities = env.join_entities();
                (!entities.is_empty()).then(|| {
                    (
                        source.to_owned(),
                        entities.into_iter().map(str::to_owned).collect::<Vec<_>>(),
                    )
                })
            }
            None => None,
        };
        if env.is_entityless() {
            self.state.counters.raw_rows += 1;
            out.raw.push(RawRow {
                event_id: env.event_id,
                source: env.source,
                entities: env.entities,
                ts_ms: env.ts_ms,
                payload: env.payload,
            });
            return;
        }
        let Some((source, entities)) = joinable else {
            self.state.counters.unjoinable += 1;
            return;
        };
        let ts = env.ts_ms;
        let window = self.window_ms;
        let keep_all = self.policy == LatePolicy::Keep;
        let watermark = self.watermark();
        if !keep_all && watermark.is_some_and(|wm| ts < wm) {
            self.state.counters.late_dropped += 1;
            return;
        }
        let evict_below = if keep_all {
            None
        } else {
            watermark.map(|wm| wm.saturating_sub(window))
        };
        for entity in entities {
            let list = self.state.entries.entry(entity.clone()).or_default();
            if let Some(threshold) = evict_below {
                list.retain(|e| e.ts >= threshold);
            }
            for other in list.iter() {
                if other.source != source && other.ts.abs_diff(ts) <= window as u64 {
                    let (a, b) = if other.source < source {
                        (&other.event_id, &env.event_id)
                    } else {
                        (&env.event_id, &other.event_id)
                    };
                    out.pairs.push(PairRow {
                        entity: entity.clone(),
                        a_id: a.clone(),
                        b_id: b.clone(),
                        ts_ms: other.ts.max(ts),
                    });
                    self.state.counters.correlated_rows += 1;
                }
            }
            list.push(Entry {
                ts,
                source: source.clone(),
                event_id: env.event_id.clone(),
            });
        }
        self.state.max_ts = Some(self.state.max_ts.map_or(ts, |m| m.max(ts)));
    }

    /// Drop every entry that can no longer pair. Run before checkpointing so
    /// the persisted state stays small.
    pub fn sweep(&mut self) {
        if self.policy == LatePolicy::Keep {
            return;
        }
        let Some(threshold) = self.watermark().map(|wm| wm.saturating_sub(self.window_ms)) else {
            return;
        };
        self.state.entries.retain(|_, list| {
            list.retain(|e| e.ts >= threshold);
            !list.is_empty()
        });
    }

    pub fn take_state(&mut self) -> EngineState {
        std::mem::take(&mut self.state)
    }

    pub fn restore(&mut self, state: EngineState) {
        self.state = state;
    }
}
