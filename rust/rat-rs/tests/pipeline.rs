//! End-to-end tests of the file pipeline, the commit protocol, the SQL read
//! side, and the Iggy HTTP source (against an in-process mock server).

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use rat_rs::checkpoint::{Checkpoint, Position};
use rat_rs::digest;
use rat_rs::engine::LatePolicy;
use rat_rs::generate::{GenOptions, generate};
use rat_rs::iggy::{IggyConfig, IggySource, Starting};
use rat_rs::pipeline::{self, Fetch, RunSummary, Settings, Source};
use rat_rs::sink::DtZone;
use rat_rs::source_file::FileSource;
use tempfile::TempDir;

fn settings(root: &Path, batch_max: usize) -> Settings {
    Settings {
        sink_dir: root.join("sink"),
        checkpoint_dir: root.join("checkpoint"),
        window_ms: 600_000,
        late_policy: LatePolicy::Drop,
        dt_zone: DtZone::Utc,
        batch_max,
        trigger: Duration::from_millis(10),
    }
}

fn process(s: &Settings, input: &Path) -> anyhow::Result<RunSummary> {
    let input = input.canonicalize()?;
    let resumed = pipeline::resume(s, format!("file:{}", input.display()))?;
    let mut source = FileSource::open(&input, &resumed.position, false)?;
    pipeline::run(s, resumed, &mut source)
}

fn gen_file(dir: &Path, opts: &GenOptions) -> PathBuf {
    let path = dir.join("input.jsonl");
    let mut f = fs::File::create(&path).unwrap();
    generate(opts, &mut f).unwrap();
    path
}

fn messy_opts(events: u64) -> GenOptions {
    GenOptions {
        events,
        entity_pool: 50,
        step_ms: 1_000,
        disorder_ms: 120_000,
        late: 0.03,
        late_by_ms: 1_500_000,
        invalid: 0.02,
        ..GenOptions::default()
    }
}

#[test]
fn batching_and_restart_points_do_not_change_output() {
    let tmp = TempDir::new().unwrap();
    let input = gen_file(tmp.path(), &messy_opts(3_000));

    let one = tmp.path().join("one");
    let s1 = settings(&one, 100_000);
    let r1 = process(&s1, &input).unwrap();
    let d1 = digest::compute(&s1.sink_dir).unwrap();
    assert_eq!(r1.records_read, 3_000);
    assert!(
        r1.records_rejected > 0 && r1.late_dropped > 0 && r1.correlated_rows > 0 && r1.raw_rows > 0
    );
    assert_eq!(d1.correlated_rows, r1.correlated_rows);
    assert_eq!(d1.raw_rows, r1.raw_rows);
    assert_eq!(d1.correlated_duplicates, 0, "a pair was emitted twice");

    // Tiny batches: many commits, same rows.
    let small = tmp.path().join("small");
    let s2 = settings(&small, 7);
    let r2 = process(&s2, &input).unwrap();
    assert!(r2.batches > 400);
    assert_eq!(digest::compute(&s2.sink_dir).unwrap().digest, d1.digest);

    // Restart in the middle: process a prefix, then the full file.
    let split = tmp.path().join("split");
    let s3 = settings(&split, 250);
    let content = fs::read(&input).unwrap();
    let cut = content
        .iter()
        .enumerate()
        .filter(|(_, b)| **b == b'\n')
        .nth(1_234)
        .unwrap()
        .0
        + 1;
    let growing = split.join("growing.jsonl");
    fs::create_dir_all(&split).unwrap();
    fs::write(&growing, &content[..cut]).unwrap();
    let first = process(&s3, &growing).unwrap();
    assert_eq!(first.records_read, 1_235);
    fs::OpenOptions::new()
        .append(true)
        .open(&growing)
        .unwrap()
        .write_all(&content[cut..])
        .unwrap();
    let second = process(&s3, &growing).unwrap();
    assert_eq!(first.records_read + second.records_read, 3_000);
    assert_eq!(second.first_batch_id, first.last_batch_id + 1);
    assert_eq!(digest::compute(&s3.sink_dir).unwrap().digest, d1.digest);
}

#[test]
fn rerun_after_completion_is_a_no_op() {
    let tmp = TempDir::new().unwrap();
    let input = gen_file(tmp.path(), &messy_opts(500));
    let s = settings(tmp.path(), 64);
    process(&s, &input).unwrap();
    let before = digest::compute(&s.sink_dir).unwrap();
    let again = process(&s, &input).unwrap();
    assert_eq!(again.records_read, 0);
    assert_eq!(again.batches, 0);
    assert_eq!(digest::compute(&s.sink_dir).unwrap(), before);
}

#[test]
fn keep_policy_equals_drop_policy_when_nothing_is_late() {
    let tmp = TempDir::new().unwrap();
    let opts = GenOptions {
        events: 2_000,
        entity_pool: 30,
        ..GenOptions::default()
    };
    let input = gen_file(tmp.path(), &opts);
    let a = settings(&tmp.path().join("drop"), 100);
    let mut b = settings(&tmp.path().join("keep"), 100);
    b.late_policy = LatePolicy::Keep;
    let ra = process(&a, &input).unwrap();
    process(&b, &input).unwrap();
    assert_eq!(ra.late_dropped, 0);
    assert_eq!(
        digest::compute(&a.sink_dir).unwrap().digest,
        digest::compute(&b.sink_dir).unwrap().digest
    );
}

fn read_checkpoint(s: &Settings) -> Checkpoint {
    serde_json::from_slice(&fs::read(s.checkpoint_dir.join("checkpoint.json")).unwrap()).unwrap()
}

#[test]
fn crash_after_commit_before_rename_rolls_forward() {
    let tmp = TempDir::new().unwrap();
    let input = gen_file(tmp.path(), &messy_opts(800));
    let s = settings(tmp.path(), 300);
    process(&s, &input).unwrap();
    let expected = digest::compute(&s.sink_dir).unwrap();

    // Undo the post-commit renames of the last batch, as if the process died
    // right after writing checkpoint.json.
    let cp = read_checkpoint(&s);
    assert!(!cp.pending.is_empty());
    for f in &cp.pending {
        fs::rename(&f.fin, &f.tmp).unwrap();
    }
    assert_ne!(digest::compute(&s.sink_dir).unwrap(), expected);

    let rerun = process(&s, &input).unwrap();
    assert_eq!(rerun.records_read, 0);
    assert_eq!(digest::compute(&s.sink_dir).unwrap(), expected);
}

#[test]
fn crash_before_commit_discards_staged_files_and_replays() {
    let tmp = TempDir::new().unwrap();
    let input = gen_file(tmp.path(), &messy_opts(800));

    let reference = settings(&tmp.path().join("ref"), 300);
    process(&reference, &input).unwrap();
    let expected = digest::compute(&reference.sink_dir).unwrap();

    // Commit a prefix, then leave behind staged files of a batch that never committed.
    let s = settings(&tmp.path().join("crash"), 300);
    let content = fs::read_to_string(&input).unwrap();
    let prefix: String = content
        .lines()
        .take(300)
        .map(|l| format!("{l}\n"))
        .collect();
    let file = tmp.path().join("crash-input.jsonl");
    fs::write(&file, &prefix).unwrap();
    process(&s, &file).unwrap();
    let orphan_dir = s.sink_dir.join("correlated").join("dt=2023-11-14");
    fs::create_dir_all(&orphan_dir).unwrap();
    let orphan = orphan_dir.join(".part-000000000002-00000.snappy.parquet.tmp");
    fs::write(&orphan, b"half-written").unwrap();

    fs::write(&file, &content).unwrap();
    process(&s, &file).unwrap();
    assert!(!orphan.exists(), "uncommitted staged file must be deleted");
    assert_eq!(
        digest::compute(&s.sink_dir).unwrap().digest,
        expected.digest
    );
}

#[test]
fn changed_settings_or_missing_checkpoint_are_refused() {
    let tmp = TempDir::new().unwrap();
    let input = gen_file(tmp.path(), &messy_opts(200));
    let s = settings(tmp.path(), 50);
    process(&s, &input).unwrap();

    let mut other = s.clone();
    other.window_ms = 60_000;
    let err = process(&other, &input).unwrap_err().to_string();
    assert!(err.contains("different settings"), "{err}");

    let mut fresh = s.clone();
    fresh.checkpoint_dir = tmp.path().join("other-checkpoint");
    let err = process(&fresh, &input).unwrap_err().to_string();
    assert!(err.contains("would duplicate output"), "{err}");
}

#[test]
fn truncated_input_is_refused() {
    let tmp = TempDir::new().unwrap();
    let input = gen_file(tmp.path(), &messy_opts(200));
    let s = settings(tmp.path(), 50);
    process(&s, &input).unwrap();
    fs::write(&input, b"{}\n").unwrap();
    let err = process(&s, &input).unwrap_err().to_string();
    assert!(err.contains("truncated or replaced"), "{err}");
}

#[test]
fn second_writer_is_locked_out() {
    let tmp = TempDir::new().unwrap();
    let input = gen_file(tmp.path(), &messy_opts(10));
    let s = settings(tmp.path(), 50);
    let canonical = input.canonicalize().unwrap();
    let held = pipeline::resume(&s, format!("file:{}", canonical.display())).unwrap();
    let err = pipeline::resume(&s, format!("file:{}", canonical.display()))
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("locked"), "{err}");
    drop(held);
    assert!(pipeline::resume(&s, format!("file:{}", canonical.display())).is_ok());
}

#[test]
fn invalid_lines_are_counted_by_reason_and_skipped() {
    let tmp = TempDir::new().unwrap();
    let input = tmp.path().join("bad.jsonl");
    let lines = [
        r#"{"event_id":"hn:1","source":"hn","entities":["AAPL"],"ts_ms":1700000000000,"payload":{"t":1}}"#,
        "{oops",
        "",
        "   ",
        "[1,2,3]",
        r#"{"source":"hn","ts_ms":1}"#,
        r#"{"event_id":"x","ts_ms":"1700000000000"}"#,
        r#"{"event_id":"rss:1","source":"rss","entities":["AAPL"],"ts_ms":1700000030000,"payload":"plain"}"#,
        "\u{feff}garbage",
        r#"{"event_id":"rss:2","source":"rss","entities":[],"ts_ms":1700000030000}"#,
    ];
    let mut body = lines.join("\n").into_bytes();
    body.extend_from_slice(b"\n\xff\xfe\n"); // invalid UTF-8
    body.extend_from_slice(br#"{"event_id":"tail","source":"x","entities":[],"ts_ms":5}"#); // no final newline
    fs::write(&input, body).unwrap();

    let s = settings(tmp.path(), 4);
    let r = process(&s, &input).unwrap();
    assert_eq!(r.records_read, 10); // blank lines are not records
    assert_eq!(r.records_rejected, 6);
    let expect: BTreeMap<String, u64> = [
        ("malformed", 3),
        ("not_object", 1),
        ("missing_event_id", 1),
        ("missing_ts_ms", 1),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect();
    assert_eq!(r.rejected_by_reason, expect);
    assert_eq!(r.correlated_rows, 1);
    assert_eq!(r.raw_rows, 2);

    let (raw, corr, _) = digest::canonical_rows(&s.sink_dir).unwrap();
    assert_eq!(
        corr,
        vec![r#"["correlated","2023-11-14","AAPL","hn:1","rss:1",1700000030000]"#]
    );
    assert!(
        raw.iter().any(|r| r.contains(r#""tail""#)),
        "unterminated last line must be read at EOF"
    );
    // Hive-style layout, partition values only in the path.
    assert!(
        s.sink_dir
            .join("events_raw/dt=2023-11-14/source=rss")
            .is_dir()
    );
    assert!(s.sink_dir.join("correlated/dt=2023-11-14").is_dir());
}

#[test]
fn hostile_source_names_are_escaped_in_partition_paths() {
    let tmp = TempDir::new().unwrap();
    let input = tmp.path().join("in.jsonl");
    fs::write(
        &input,
        concat!(
            r#"{"event_id":"1","source":"../../etc","entities":[],"ts_ms":0}"#,
            "\n",
            r#"{"event_id":"2","source":"","entities":[],"ts_ms":0}"#,
            "\n",
            r#"{"event_id":"3","entities":[],"ts_ms":0}"#,
            "\n"
        ),
    )
    .unwrap();
    let s = settings(tmp.path(), 10);
    process(&s, &input).unwrap();
    let raw = s.sink_dir.join("events_raw/dt=1970-01-01");
    assert!(raw.join("source=..%2F..%2Fetc").is_dir());
    assert!(raw.join("source=__HIVE_DEFAULT_PARTITION__").is_dir());
    let (rows, _, _) = digest::canonical_rows(&s.sink_dir).unwrap();
    assert!(rows.iter().any(|r| r.contains(r#""../../etc""#)));
}

#[cfg(feature = "sql")]
#[test]
fn sql_read_side_sees_committed_rows_with_partition_columns() {
    let tmp = TempDir::new().unwrap();
    let input = gen_file(tmp.path(), &messy_opts(1_500));
    let s = settings(tmp.path(), 200);
    let r = process(&s, &input).unwrap();
    // a stray staged file must stay invisible to SQL
    let stray = s
        .sink_dir
        .join("correlated/dt=2023-11-14/.part-999-0.snappy.parquet.tmp");
    fs::write(&stray, b"x").unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let ctx = rat_rs::query::context(&s.sink_dir).await.unwrap();
        let count = |sql: &'static str| {
            let ctx = ctx.clone();
            async move {
                let batches = rat_rs::query::sql(&ctx, sql).await.unwrap();
                let col = batches[0].column(0);
                col.as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .unwrap()
                    .value(0) as u64
            }
        };
        assert_eq!(
            count("SELECT count(*) FROM correlated").await,
            r.correlated_rows
        );
        assert_eq!(count("SELECT count(*) FROM rat_events").await, r.raw_rows);
        assert!(count("SELECT count(DISTINCT source) FROM rat_events").await >= 3);
        assert_eq!(
            count(
                "SELECT count(*) FROM correlated WHERE dt <> '2023-11-14' AND dt <> '2023-11-15'"
            )
            .await,
            0
        );
        for (_, sql) in rat_rs::query::LEGACY_QUERIES {
            let batches = rat_rs::query::sql(&ctx, sql).await.unwrap();
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 10);
            assert!(
                !rat_rs::query::render(&batches, rat_rs::query::Format::Table)
                    .unwrap()
                    .is_empty()
            );
        }
    });
}

// ---------------------------------------------------------------- Iggy mock

#[derive(Default)]
struct MockIggy {
    /// topic -> partition -> payloads
    topics: BTreeMap<String, Vec<Vec<Vec<u8>>>>,
    tokens_issued: u32,
    /// Reject the next authenticated request with 401 (expired token).
    expire_next: bool,
    bad_payload_once: bool,
    polls: Vec<String>,
}

fn respond(stream: &mut TcpStream, status: &str, body: &str) {
    let _ = write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
}

fn serve(mock: Arc<Mutex<MockIggy>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request_line = String::new();
            reader.read_line(&mut request_line).unwrap();
            let mut auth = String::new();
            let mut length = 0usize;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                let lower = line.to_ascii_lowercase();
                if let Some(v) = lower.strip_prefix("authorization:") {
                    auth = v.trim().to_owned();
                }
                if let Some(v) = lower.strip_prefix("content-length:") {
                    length = v.trim().parse().unwrap();
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let mut parts = request_line.split_whitespace();
            let method = parts.next().unwrap_or("").to_owned();
            let target = parts.next().unwrap_or("").to_owned();
            let (path, query) = target.split_once('?').unwrap_or((&target, ""));
            let mut m = mock.lock().unwrap();
            if method == "POST" && path == "/users/login" {
                m.tokens_issued += 1;
                let body = format!(
                    r#"{{"user_id":0,"access_token":{{"token":"tok{}","expiry":0}}}}"#,
                    m.tokens_issued
                );
                respond(&mut stream, "200 OK", &body);
                continue;
            }
            if auth != format!("bearer tok{}", m.tokens_issued) || m.expire_next {
                m.expire_next = false;
                respond(
                    &mut stream,
                    "401 Unauthorized",
                    r#"{"id":40,"code":"unauthenticated"}"#,
                );
                continue;
            }
            let segs: Vec<&str> = path.trim_start_matches('/').split('/').collect();
            match segs.as_slice() {
                ["streams", "rat"] => {
                    let topics: Vec<String> = m
                        .topics
                        .keys()
                        .enumerate()
                        .map(|(i, name)| {
                            format!(r#"{{"id":{i},"name":"{name}","partitions_count":2}}"#)
                        })
                        .collect();
                    respond(
                        &mut stream,
                        "200 OK",
                        &format!(r#"{{"id":1,"name":"rat","topics":[{}]}}"#, topics.join(",")),
                    );
                }
                ["streams", "rat", "topics", topic] => match m.topics.get(*topic) {
                    None => respond(&mut stream, "404 Not Found", "{}"),
                    Some(parts) => {
                        let ps: Vec<String> = parts
                            .iter()
                            .enumerate()
                            .map(|(i, msgs)| {
                                format!(
                                    r#"{{"id":{i},"current_offset":{},"messages_count":{},"segments_count":1}}"#,
                                    msgs.len().saturating_sub(1),
                                    msgs.len()
                                )
                            })
                            .collect();
                        respond(
                            &mut stream,
                            "200 OK",
                            &format!(r#"{{"name":"{topic}","partitions":[{}]}}"#, ps.join(",")),
                        );
                    }
                },
                ["streams", "rat", "topics", topic, "messages"] => {
                    m.polls.push(format!("{topic}?{query}"));
                    let q: BTreeMap<&str, &str> = query
                        .split('&')
                        .filter_map(|kv| kv.split_once('='))
                        .collect();
                    assert_eq!(q["kind"], "offset");
                    assert_eq!(q["auto_commit"], "false");
                    let partition: usize = q["partition_id"].parse().unwrap();
                    let from: usize = q["value"].parse().unwrap();
                    let count: usize = q["count"].parse().unwrap();
                    let bad_payload = std::mem::take(&mut m.bad_payload_once);
                    let msgs = &m.topics[*topic][partition];
                    let out: Vec<String> = msgs
                        .iter()
                        .enumerate()
                        .skip(from)
                        .take(count)
                        .map(|(offset, payload)| {
                            format!(
                                r#"{{"header":{{"checksum":1,"id":340282366920938463463374607431768211455,"offset":{offset},"timestamp":1,"origin_timestamp":1,"user_headers_length":0,"payload_length":{},"reserved":0}},"payload":"{}","user_headers":[]}}"#,
                                payload.len(),
                                if bad_payload && offset == 1 { "%%%invalid-base64".into() } else { STANDARD.encode(payload) }
                            )
                        })
                        .collect();
                    respond(
                        &mut stream,
                        "200 OK",
                        &format!(
                            r#"{{"partition_id":{partition},"current_offset":{},"count":{},"messages":[{}]}}"#,
                            msgs.len().saturating_sub(1),
                            out.len(),
                            out.join(",")
                        ),
                    );
                }
                _ => respond(&mut stream, "404 Not Found", "{}"),
            }
        }
    });
    format!("http://{addr}")
}

/// Passes the first Idle through (so the pipeline commits a moved position),
/// then ends the run.
struct UntilIdle(IggySource, bool);

impl Source for UntilIdle {
    fn position(&self) -> Position {
        self.0.position()
    }
    fn fetch(&mut self, max: usize) -> anyhow::Result<Fetch> {
        Ok(match self.0.fetch(max)? {
            Fetch::Idle if self.1 => Fetch::End,
            Fetch::Idle => {
                self.1 = true;
                Fetch::Idle
            }
            other => other,
        })
    }
}

fn iggy_cfg(url: &str, starting: Starting) -> IggyConfig {
    IggyConfig {
        url: url.to_owned(),
        username: "iggy".into(),
        password: "iggy".into(),
        pat: None,
        stream: "rat".into(),
        topic_pattern: r"events\..*".into(),
        topic_exclude: r"\.dlq$".into(),
        poll_count: 3,
        consumer_id: "rat-engine".into(),
        starting,
        request_timeout: Duration::from_secs(5),
    }
}

fn consume(s: &Settings, url: &str, starting: Starting) -> RunSummary {
    let resumed = pipeline::resume(s, "iggy:rat".into()).unwrap();
    let source = IggySource::open(iggy_cfg(url, starting), &resumed.position).unwrap();
    pipeline::run(s, resumed, &mut UntilIdle(source, false)).unwrap()
}

fn line(id: &str, source: &str, entity: &str, ts: i64) -> Vec<u8> {
    format!(r#"{{"event_id":"{id}","source":"{source}","entities":["{entity}"],"ts_ms":{ts},"payload":{{}}}}"#).into_bytes()
}

#[test]
fn iggy_source_reads_all_partitions_skips_dlq_and_resumes_from_checkpoint() {
    let t0 = 1_700_000_000_000i64;
    let mock = Arc::new(Mutex::new(MockIggy::default()));
    {
        let mut m = mock.lock().unwrap();
        m.topics.insert(
            "events.hn".into(),
            vec![
                (0..5)
                    .map(|i| line(&format!("hn:{i}"), "hn", "AAPL", t0 + i))
                    .collect(),
                vec![line("hn:p1", "hn", "MSFT", t0)],
            ],
        );
        m.topics.insert(
            "events.rss".into(),
            vec![vec![line("rss:0", "rss", "AAPL", t0 + 100)], vec![]],
        );
        m.topics.insert(
            "events.rss.dlq".into(),
            vec![vec![b"{not an envelope".to_vec()], vec![]],
        );
        m.topics.insert(
            "other".into(),
            vec![vec![line("o:0", "other", "AAPL", t0)], vec![]],
        );
    }
    let url = serve(mock.clone());
    let tmp = TempDir::new().unwrap();
    let s = settings(tmp.path(), 4);

    let first = consume(&s, &url, Starting::Earliest);
    assert_eq!(first.records_read, 7);
    assert_eq!(first.records_rejected, 0, "DLQ topic must not be read");
    assert_eq!(first.correlated_rows, 5); // 5 hn x 1 rss on AAPL
    assert!(
        mock.lock()
            .unwrap()
            .polls
            .iter()
            .all(|p| !p.contains("dlq") && !p.starts_with("other"))
    );

    let cp = read_checkpoint(&s);
    let Position::Iggy { offsets } = &cp.position else {
        panic!("{:?}", cp.position)
    };
    assert_eq!(offsets["events.hn"][&0], 5);
    assert_eq!(offsets["events.hn"][&1], 1);
    assert_eq!(offsets["events.rss"][&0], 1);

    // New messages plus an expired token: re-login, read only what is new.
    {
        let mut m = mock.lock().unwrap();
        m.topics.get_mut("events.rss").unwrap()[1].push(line("rss:p1", "rss", "MSFT", t0 + 5));
        m.expire_next = true;
    }
    let second = consume(&s, &url, Starting::Earliest);
    assert_eq!(second.records_read, 1);
    assert_eq!(second.correlated_rows, 1);
    assert!(mock.lock().unwrap().tokens_issued >= 3);

    let d = digest::compute(&s.sink_dir).unwrap();
    assert_eq!(d.correlated_rows, 6);
    assert_eq!(d.correlated_duplicates, 0);
}

#[test]
fn iggy_latest_skips_history_on_first_run_only() {
    let t0 = 1_700_000_000_000i64;
    let mock = Arc::new(Mutex::new(MockIggy::default()));
    mock.lock().unwrap().topics.insert(
        "events.hn".into(),
        vec![vec![line("hn:old", "hn", "AAPL", t0)], vec![]],
    );
    let url = serve(mock.clone());
    let tmp = TempDir::new().unwrap();
    let s = settings(tmp.path(), 100);

    // First run: history is skipped, and the resolved end offsets are committed.
    let first = consume(&s, &url, Starting::Latest);
    assert_eq!(first.records_read, 0);
    assert_eq!(
        first.batches, 1,
        "resolved starting offsets must be checkpointed"
    );

    {
        let mut m = mock.lock().unwrap();
        m.topics.get_mut("events.hn").unwrap()[0].push(line("hn:new", "hn", "AAPL", t0 + 1));
        // A topic that appears after the first run starts from earliest (Spark parity).
        m.topics.insert(
            "events.rss".into(),
            vec![vec![line("rss:0", "rss", "AAPL", t0 + 2)]],
        );
    }
    let second = consume(&s, &url, Starting::Latest);
    assert_eq!(second.records_read, 2);
    assert_eq!(second.correlated_rows, 1);
    let (_, corr, _) = digest::canonical_rows(&s.sink_dir).unwrap();
    assert_eq!(corr.len(), 1);
    assert!(corr[0].contains(r#""hn:new","rss:0""#), "{corr:?}");
}

#[test]
fn reset_validates_all_paths_before_deleting_anything() {
    let tmp = TempDir::new().unwrap();
    let sink = tmp.path().join("sink");
    fs::create_dir_all(&sink).unwrap();
    fs::write(sink.join("keep"), b"existing data").unwrap();
    assert!(pipeline::reset_dirs(&[&sink, Path::new("/etc")]).is_err());
    assert!(sink.join("keep").exists());
    assert!(pipeline::reset_dirs(&[Path::new("/tmp/../home")]).is_err());
    assert!(pipeline::reset_dirs(&[Path::new("/tmp")]).is_err());
}

#[cfg(unix)]
#[test]
fn reset_refuses_symlink_escape() {
    let tmp = TempDir::new().unwrap();
    let link = tmp.path().join("escape");
    std::os::unix::fs::symlink("/", &link).unwrap();
    assert!(pipeline::reset_dirs(&[&link.join("etc")]).is_err());
    assert!(link.is_symlink());
}

#[test]
fn failed_poll_does_not_commit_partially_decoded_offsets() {
    let t0 = 1_700_000_000_000i64;
    let mock = Arc::new(Mutex::new(MockIggy::default()));
    {
        let mut m = mock.lock().unwrap();
        m.bad_payload_once = true;
        m.topics.insert(
            "events.hn".into(),
            vec![
                vec![
                    line("hn:0", "hn", "AAPL", t0),
                    line("hn:1", "hn", "AAPL", t0 + 1),
                ],
                vec![],
            ],
        );
        m.topics.insert(
            "events.rss".into(),
            vec![vec![line("rss:0", "rss", "AAPL", t0 + 2)], vec![]],
        );
    }
    let url = serve(mock);
    let tmp = TempDir::new().unwrap();
    let s = settings(tmp.path(), 100);
    let result = consume(&s, &url, Starting::Earliest);
    assert_eq!(result.records_read, 3);
    assert_eq!(result.correlated_rows, 2);
    assert_eq!(digest::compute(&s.sink_dir).unwrap().correlated_rows, 2);
}

#[test]
fn streaming_source_waits_between_committed_batches() {
    struct LiveFile {
        inner: FileSource,
        waits: Vec<Duration>,
    }
    impl Source for LiveFile {
        fn is_streaming(&self) -> bool {
            true
        }
        fn position(&self) -> Position {
            self.inner.position()
        }
        fn fetch(&mut self, max: usize) -> anyhow::Result<Fetch> {
            self.inner.fetch(max)
        }
        // Capture the requested pacing without making a test wait an hour.
        fn idle(&mut self, duration: Duration) {
            self.waits.push(duration);
        }
    }
    let tmp = TempDir::new().unwrap();
    let input = gen_file(tmp.path(), &messy_opts(60));
    let mut s = settings(tmp.path(), 20);
    s.trigger = Duration::from_secs(3600);
    let resumed = pipeline::resume(&s, format!("file:{}", input.display())).unwrap();
    let mut live = LiveFile {
        inner: FileSource::open(&input, &resumed.position, false).unwrap(),
        waits: Vec::new(),
    };
    let result = pipeline::run(&s, resumed, &mut live).unwrap();
    assert_eq!(result.records_read, 60);
    assert_eq!(result.batches, 3);
    assert!(
        !live.waits.is_empty(),
        "live batches must respect RAT_TRIGGER"
    );
    assert!(live.waits.iter().all(|wait| *wait <= s.trigger));
}
