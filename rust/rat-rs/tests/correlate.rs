//! Port of spark/src/test/scala/rat/CorrelateSpec.scala, plus the streaming
//! (watermark / out-of-order) behaviour the batch spec cannot cover.

use rat_rs::engine::{Engine, LatePolicy, Output, PairRow};
use rat_rs::envelope::Envelope;

const T0: i64 = 1_700_000_000_000;
const W: i64 = 600_000; // "10 minutes"

fn env(id: &str, source: Option<&str>, entities: Option<&[Option<&str>]>, ts: i64) -> Envelope {
    Envelope {
        event_id: id.to_owned(),
        source: source.map(str::to_owned),
        entities: entities.map(|list| list.iter().map(|e| e.map(str::to_owned)).collect()),
        ts_ms: ts,
        payload: None,
    }
}

fn e(id: &str, source: &str, entities: &[&str], ts: i64) -> Envelope {
    let list: Vec<Option<&str>> = entities.iter().map(|s| Some(*s)).collect();
    env(id, Some(source), Some(&list), ts)
}

fn run(policy: LatePolicy, events: Vec<Envelope>) -> (Vec<PairRow>, Engine) {
    let mut engine = Engine::new(W, policy, Default::default());
    let mut out = Output::default();
    for ev in events {
        engine.process(ev, &mut out);
    }
    let mut pairs = out.pairs;
    pairs.sort_by(|a, b| (&a.a_id, &a.b_id, &a.entity).cmp(&(&b.a_id, &b.b_id, &b.entity)));
    (pairs, engine)
}

/// Same contract as CorrelateSpec: batch semantics (no watermark).
fn correlate(events: Vec<Envelope>) -> Vec<PairRow> {
    let (batch, _) = run(LatePolicy::Keep, events.clone());
    // Every spec case is also in watermark range, so streaming must agree.
    let (stream, _) = run(LatePolicy::Drop, events);
    assert_eq!(batch, stream, "streaming and batch semantics diverged");
    batch
}

fn ids(rows: &[PairRow]) -> Vec<(&str, &str)> {
    rows.iter()
        .map(|r| (r.a_id.as_str(), r.b_id.as_str()))
        .collect()
}

#[test]
fn any_two_sources_pair_on_a_shared_entity_inside_the_window() {
    let rows = correlate(vec![
        e("b1", "blog", &["ETH"], T0),
        e("p1", "podcast", &["ETH"], T0 + 60_000),
    ]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].entity, "ETH");
    assert_eq!(rows[0].a_id, "b1"); // lexicographically smaller source
    assert_eq!(rows[0].b_id, "p1");
    assert_eq!(rows[0].ts_ms, T0 + 60_000); // the later of the pair
}

#[test]
fn hn_and_rss_pair_exactly_once_a_from_hn_b_from_rss() {
    let rows = correlate(vec![
        e("e:hn:1", "hn", &["AAPL"], T0),
        e("e:rss:1", "rss", &["AAPL"], T0 + 30_000),
    ]);
    assert_eq!(ids(&rows), vec![("e:hn:1", "e:rss:1")]);
    assert_eq!(rows[0].ts_ms, T0 + 30_000);
}

#[test]
fn source_order_does_not_depend_on_arrival_order() {
    // rss arrives first; a_id must still be the hn record.
    let rows = correlate(vec![
        e("e:rss:1", "rss", &["AAPL"], T0 + 30_000),
        e("e:hn:1", "hn", &["AAPL"], T0),
    ]);
    assert_eq!(ids(&rows), vec![("e:hn:1", "e:rss:1")]);
    assert_eq!(rows[0].ts_ms, T0 + 30_000);
}

#[test]
fn source_order_is_bytewise_like_spark() {
    // 'Z' (0x5A) < 'a' (0x61): uppercase sorts first, as UTF8String compare does.
    let rows = correlate(vec![
        e("lower", "alpha", &["X"], T0),
        e("upper", "Zeta", &["X"], T0),
    ]);
    assert_eq!(ids(&rows), vec![("upper", "lower")]);
}

#[test]
fn no_self_or_same_source_pairs_no_mirrored_duplicates() {
    assert!(
        correlate(vec![
            e("h1", "hn", &["AAPL"], T0),
            e("h2", "hn", &["AAPL"], T0 + 1000)
        ])
        .is_empty()
    );
    assert!(correlate(vec![e("h1", "hn", &["AAPL"], T0)]).is_empty());
}

#[test]
fn three_sources_sharing_an_entity_give_c3_2_pairs() {
    let rows = correlate(vec![
        e("a1", "alpha", &["AAPL"], T0),
        e("b1", "beta", &["AAPL"], T0 + 1000),
        e("g1", "gamma", &["AAPL"], T0 + 2000),
    ]);
    assert_eq!(ids(&rows), vec![("a1", "b1"), ("a1", "g1"), ("b1", "g1")]);
}

#[test]
fn duplicate_entities_inside_one_event_do_not_multiply_output() {
    let rows = correlate(vec![
        e("h1", "hn", &["AAPL", "AAPL"], T0),
        e("r1", "rss", &["AAPL"], T0 + 1000),
    ]);
    assert_eq!(rows.len(), 1);
    let rows = correlate(vec![
        e("h1", "hn", &["AAPL", "AAPL"], T0),
        e("r1", "rss", &["AAPL", "AAPL", "AAPL"], T0 + 1000),
    ]);
    assert_eq!(rows.len(), 1);
}

#[test]
fn multiple_shared_entities_give_one_row_per_entity() {
    let rows = correlate(vec![
        e("h1", "hn", &["AAPL", "MSFT"], T0),
        e("r1", "rss", &["AAPL", "MSFT"], T0 + 1000),
    ]);
    let entities: Vec<&str> = rows.iter().map(|r| r.entity.as_str()).collect();
    assert_eq!(entities, vec!["AAPL", "MSFT"]);
    assert!(rows.iter().all(|r| r.a_id == "h1" && r.b_id == "r1"));
}

#[test]
fn pair_inside_exactly_at_and_outside_the_window() {
    let pair = |delta: i64| {
        correlate(vec![
            e("h1", "hn", &["AAPL"], T0),
            e("r1", "rss", &["AAPL"], T0 + delta),
        ])
        .len()
    };
    assert_eq!(pair(60_000), 1); // inside
    assert_eq!(pair(W), 1); // exact boundary, inclusive
    assert_eq!(pair(-W), 1); // boundary from the other side
    assert_eq!(pair(W + 1), 0); // one ms past
    assert_eq!(pair(-W - 1), 0);
}

#[test]
fn null_or_blank_source_and_entity_rows_are_excluded() {
    let rows = correlate(vec![
        env("n1", None, Some(&[Some("AAPL")]), T0),
        env("n2", Some("  "), Some(&[Some("AAPL")]), T0 + 1000),
        e("h1", "hn", &["AAPL"], T0 + 2000),
        e("r1", "rss", &["AAPL"], T0 + 3000),
    ]);
    assert_eq!(ids(&rows), vec![("h1", "r1")]);

    let rows = correlate(vec![
        env(
            "h1",
            Some("hn"),
            Some(&[None, Some(""), Some("  "), Some("AAPL")]),
            T0,
        ),
        e("r1", "rss", &["AAPL"], T0 + 1000),
    ]);
    assert_eq!(rows.len(), 1);

    let rows = correlate(vec![
        env("h1", Some("hn"), None, T0),
        e("r1", "rss", &["AAPL"], T0 + 1000),
    ]);
    assert!(rows.is_empty());
}

#[test]
fn raw_branch_takes_entityless_records_only() {
    let mut engine = Engine::new(W, LatePolicy::Drop, Default::default());
    let mut out = Output::default();
    engine.process(env("a", Some("rss"), Some(&[]), T0), &mut out);
    engine.process(env("b", Some("rss"), None, T0), &mut out); // null entities count as entity-less
    engine.process(env("c", None, Some(&[]), T0), &mut out); // raw even without a source
    engine.process(e("d", "rss", &["AAPL"], T0), &mut out);
    engine.process(env("f", Some("rss"), Some(&[Some(" ")]), T0), &mut out); // blank-only: neither sink
    let raw: Vec<&str> = out.raw.iter().map(|r| r.event_id.as_str()).collect();
    assert_eq!(raw, vec!["a", "b", "c"]);
    assert_eq!(engine.state().counters.unjoinable, 1);
}

#[test]
fn out_of_order_within_the_watermark_still_pairs() {
    // max advances to T0+9m; a record 9m behind max is not late.
    let (rows, engine) = run(
        LatePolicy::Drop,
        vec![
            e("r1", "rss", &["AAPL"], T0 + 540_000),
            e("x", "stocks", &["OTHER"], T0 + 540_000),
            e("h1", "hn", &["AAPL"], T0),
        ],
    );
    assert_eq!(ids(&rows), vec![("h1", "r1")]);
    assert_eq!(engine.state().counters.late_dropped, 0);
}

#[test]
fn boundary_pair_arriving_out_of_order_is_kept() {
    // Watermark = max - W = T0 exactly; ts == watermark is not late.
    let (rows, engine) = run(
        LatePolicy::Drop,
        vec![
            e("r1", "rss", &["AAPL"], T0 + W),
            e("h1", "hn", &["AAPL"], T0),
        ],
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(engine.state().counters.late_dropped, 0);
}

#[test]
fn late_records_are_dropped_from_the_join_but_keep_policy_keeps_them() {
    let events = vec![
        e("h1", "hn", &["AAPL"], T0),
        e("x", "stocks", &["OTHER"], T0 + 2 * W), // watermark -> T0 + W
        e("r1", "rss", &["AAPL"], T0 + W - 1),    // behind the watermark: late
    ];
    let (rows, engine) = run(LatePolicy::Drop, events.clone());
    assert!(rows.is_empty());
    assert_eq!(engine.state().counters.late_dropped, 1);

    let (rows, _) = run(LatePolicy::Keep, events);
    assert_eq!(ids(&rows), vec![("h1", "r1")]);
}

#[test]
fn eviction_bounds_state_without_changing_output() {
    let mut events = Vec::new();
    for i in 0..2_000i64 {
        let src = ["hn", "rss", "stocks"][(i % 3) as usize];
        events.push(e(
            &format!("{src}:{i}"),
            src,
            &[&format!("E{}", i % 7)],
            T0 + i * 10_000,
        ));
    }
    let (dropped, mut engine) = run(LatePolicy::Drop, events.clone());
    let (kept, _) = run(LatePolicy::Keep, events);
    assert_eq!(
        dropped, kept,
        "in-order input must give identical pairs under both policies"
    );
    engine.sweep();
    // Window is 10 min = 60 events at 10s spacing; state holds ~2 windows at most.
    assert!(
        engine.state_entries() <= 130,
        "state not bounded: {}",
        engine.state_entries()
    );
}

#[test]
fn extreme_timestamps_do_not_overflow_or_create_false_pairs() {
    let (rows, _) = run(
        LatePolicy::Keep,
        vec![
            e("old", "hn", &["AAPL"], i64::MIN),
            e("new", "rss", &["AAPL"], i64::MAX),
        ],
    );
    assert!(rows.is_empty());
}
