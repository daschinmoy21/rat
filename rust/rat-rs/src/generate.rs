//! Deterministic synthetic envelopes for benchmarks and tests. The same
//! options produce a byte-identical file on any machine.

use std::io::Write;

use anyhow::Result;
use serde_json::json;

#[derive(Debug, Clone)]
pub struct GenOptions {
    pub events: u64,
    pub seed: u64,
    pub sources: Vec<String>,
    pub entity_pool: u64,
    pub start_ts_ms: i64,
    /// Mean event-time spacing between consecutive records.
    pub step_ms: i64,
    /// Max event-time jitter backwards. Values below the watermark delay
    /// produce out-of-order input that is never late.
    pub disorder_ms: i64,
    /// Share of records with no entities (raw sink).
    pub entityless: f64,
    /// Share of records pushed `late_by_ms` behind their position (late data).
    pub late: f64,
    pub late_by_ms: i64,
    /// Share of lines replaced by invalid input (malformed JSON, missing fields).
    pub invalid: f64,
}

impl Default for GenOptions {
    fn default() -> Self {
        GenOptions {
            events: 100_000,
            seed: 42,
            sources: ["hn", "lobsters", "news", "rss", "stocks"]
                .map(String::from)
                .to_vec(),
            entity_pool: 5_000,
            start_ts_ms: 1_700_000_000_000,
            step_ms: 100,
            disorder_ms: 30_000,
            entityless: 0.2,
            late: 0.0,
            late_by_ms: 1_800_000,
            invalid: 0.0,
        }
    }
}

/// SplitMix64: tiny, fast, and stable. The exact stream is part of the
/// benchmark contract, so it must not depend on an external crate's version.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    /// Uniform in [0, 1).
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }
}

pub fn generate(opts: &GenOptions, out: &mut dyn Write) -> Result<()> {
    let mut rng = Rng::new(opts.seed);
    let sources = if opts.sources.is_empty() {
        vec!["hn".to_owned()]
    } else {
        opts.sources.clone()
    };
    let mut line = Vec::with_capacity(512);
    for i in 0..opts.events {
        line.clear();
        let source = &sources[rng.below(sources.len() as u64) as usize];
        let mut ts = opts.start_ts_ms + i as i64 * opts.step_ms;
        if opts.disorder_ms > 0 {
            ts -= rng.below(opts.disorder_ms as u64 + 1) as i64;
        }
        if rng.unit() < opts.late {
            ts -= opts.late_by_ms;
        }
        let entities: Vec<String> = if rng.unit() < opts.entityless {
            Vec::new()
        } else {
            let count = 1 + rng.below(3);
            let mut list: Vec<String> = (0..count)
                .map(|_| {
                    // Squared uniform skews towards low ids: a few hot entities, a long tail.
                    let u = rng.unit();
                    format!("E{:05}", (u * u * opts.entity_pool as f64) as u64)
                })
                .collect();
            // Exercise duplicate and blank entity handling.
            if rng.unit() < 0.05 {
                list.push(list[0].clone());
            }
            if rng.unit() < 0.02 {
                list.push(" ".to_owned());
            }
            list
        };
        if rng.unit() < opts.invalid {
            match rng.below(3) {
                0 => write!(line, "{{\"event_id\":\"{source}:{i}\",\"ts_ms\":")?,
                1 => serde_json::to_writer(&mut line, &json!({"source": source, "ts_ms": ts}))?,
                _ => serde_json::to_writer(
                    &mut line,
                    &json!({"event_id": format!("{source}:{i}"), "ts_ms": ts.to_string()}),
                )?,
            }
        } else {
            serde_json::to_writer(
                &mut line,
                &json!({
                    "event_id": format!("{source}:{i}"),
                    "source": source,
                    "entities": entities,
                    "ts_ms": ts,
                    "payload": {"title": format!("synthetic {source} item {i}"), "n": i},
                }),
            )?;
        }
        line.push(b'\n');
        out.write_all(&line)?;
    }
    out.flush()?;
    Ok(())
}
