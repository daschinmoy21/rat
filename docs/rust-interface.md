# Rust port interface (`rat-rs`)

Status: **implemented**, with tests passing. The command names, flags and env
vars below are the contract. If one has to change, this file changes first.
Design, guarantees and limitations: [rust-port.md](rust-port.md).

The workspace is defined in `Cargo.toml`, with the implementation under
`rust/rat-rs/`. The JVM sources in `spark/` and `hive/` remain available for
comparison.

## Build

```bash
# rustup toolchains exist under ~/.rustup but the rustup shim is missing on this box:
export PATH="$HOME/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin:$PATH"
cargo build --locked --release -p rat-rs # binary: target/release/rat-rs
cargo test --locked -p rat-rs
```

## Deterministic benchmark

```bash
# 1. Generate a fixed input. Same --seed/--events gives a byte-identical file.
target/release/rat-rs gen --events 200000 --seed 42 --out /tmp/rat-bench/input.jsonl

# 2. Correlate it. --reset wipes sink + checkpoint first (allowed only under /tmp).
target/release/rat-rs process --input /tmp/rat-bench/input.jsonl \
    --sink-dir /tmp/rat-bench/sink --reset --dt-timezone utc
#    stdout: one JSON object, e.g.
#    {"records_read":..,"records_rejected":..,"late_dropped":..,"raw_rows":..,
#     "correlated_rows":..,"batches":..,"elapsed_ms":..,"output_digest":"<hex>"}

# Or steps 1 and 2 together:
target/release/rat-rs bench --events 200000 --seed 42 --work-dir /tmp/rat-bench
```

### Parity with the legacy export

```bash
target/release/rat-rs process --input /tmp/rat-bench-input.jsonl \
    --sink-dir /tmp/rat-rust-file --reset --dt-timezone local   # legacy JVM ran in the host zone
target/release/rat-rs digest --sink-dir /tmp/rat-legacy-export # reads Spark output too
target/release/rat-rs digest --sink-dir /tmp/rat-rust-file     # digests must match
target/release/rat-rs digest --rows --sink-dir DIR             # canonical rows, for diffing
```

The checkpoint defaults to `$RAT_SINK_DIR/checkpoints/rat-rs`. A sink dir
that holds data but has no checkpoint is refused, to avoid duplicating output.
`--reset` removes both, and only under `/tmp`.

`output_digest` is a hash over the **sorted** contents of both sinks. Two runs
over the same input with the same flags must print the same digest, however
the input was batched or restarted. Use it to check parity and replay.

The generator writes one envelope per line in the producer format
(`event_id, source, entities, ts_ms, payload`). That same file can be produced
into Kafka for the legacy Spark baseline, so both sides read identical input.

## Commands

| Command | Purpose |
|---|---|
| `rat-rs gen` | deterministic synthetic JSONL envelopes |
| `rat-rs process --input F` | finite JSONL replay through the same streaming engine; resumes from checkpoint |
| `rat-rs consume` | long-running Iggy HTTP consumer (replaces `rat.Correlate` reading Kafka) |
| `rat-rs query` | parity with `rat.Query`: latest 10 raw rows + latest 10 correlated rows |
| `rat-rs sql "<SQL>"` | arbitrary SQL (DataFusion) over tables `rat_events` and `correlated` |
| `rat-rs bench` | `gen` + `process` in one step, prints a JSON summary |
| `rat-rs digest [--rows]` | order-independent digest or canonical rows of any sink dir (Spark output too) |

## Env (shared with the legacy job where the meaning is the same)

| Var | Meaning | Default |
|---|---|---|
| `RAT_SINK_DIR` | Parquet root (`events_raw/`, `correlated/`) | `/tmp/rat` |
| `RAT_CHECKPOINT_DIR` | checkpoint dir (rat-rs owns it exclusively) | `$RAT_SINK_DIR/checkpoints/rat-rs` |
| `RAT_WATERMARK` | watermark delay **and** join half-window, Spark interval syntax | `10 minutes` |
| `RAT_STARTING_OFFSETS` | first run with no checkpoint: `earliest` or `latest` | `latest` |
| `RAT_TRIGGER` | live batch interval and idle poll interval, e.g. `2 seconds` | `1 second` |
| `RAT_BATCH_MAX` | max records per committed micro-batch | `10000` |
| `RAT_DT_TIMEZONE` | `local` (same as the Spark JVM default) or `utc` | `local` |
| `RAT_LATE_POLICY` | `drop` (bounded state) or `keep` (exact batch join, unbounded) | `drop` |
| `RAT_IGGY_URL` | Iggy HTTP base URL | `http://127.0.0.1:3000` |
| `RAT_IGGY_USERNAME` / `RAT_IGGY_PASSWORD` | login (`POST /users/login`) | `iggy` / `iggy` |
| `RAT_IGGY_PAT` | personal access token; used instead of user/password when set | unset |
| `RAT_IGGY_STREAM` | stream name or numeric id | `rat` |
| `RAT_IGGY_TOPIC_PATTERN` | topics to read, full-match regex against topic name | `events\..*` |
| `RAT_IGGY_TOPIC_EXCLUDE` | topics to skip, regex search | `\.dlq$` |
| `RAT_IGGY_POLL_COUNT` | messages per poll request | `1000` |
| `RAT_IGGY_CONSUMER_ID` | consumer id for the poll query (no server-side commits) | `rat-engine` |

The legacy topic regex `events\.(?!.*\.dlq$).*` uses a lookahead, which the
Rust regex engine does not support. PATTERN plus EXCLUDE selects the same set.

## Contract with the Iggy / producer agent

* One Iggy message is one envelope: UTF-8 JSON
  `{"event_id","source","entities","ts_ms","payload"}`. This is the Kafka
  `value` used today, byte for byte. Headers and the message key are ignored.
* One topic per source, named `events.<source>`, inside stream `RAT_IGGY_STREAM`.
  DLQ topics end in `.dlq` and are never read. The consumer re-lists topics
  every poll cycle, so a new plugin's topic is picked up without a restart.
* The consumer reads partitions by explicit offset
  (`GET /streams/{s}/topics/{t}/messages?consumer_id=..&partition_id=..&kind=offset&value=N&count=..&auto_commit=false`)
  and keeps its own offsets in its checkpoint. It **does not** depend on
  server-side consumer offsets or consumer groups, so it needs only the poll
  permission. Partition ids come from `GET /streams/{s}/topics/{t}`.
* Producers keep at-least-once semantics. A duplicate send of the same
  `event_id` is processed again, as in the legacy job (the legacy job does no
  `event_id` dedup either).

These shapes were checked against the local Iggy source at
`/home/crimxnhaze/src/iggy` (commit f6dfbdbb, `core/server/src/http.rs`,
`core/common/src/http/messages/poll_messages.rs`,
`core/common/src/types/message/{polled_messages,iggy_message,message_header}.rs`).

## consume: runtime behaviour

* It runs until SIGINT or SIGTERM, finishes the batch in flight, commits, and
  prints a JSON summary on stdout. The summary includes
  `iggy.{polls,messages,offset_gaps,errors}`.
* A broker that is down or answers 5xx is logged to stderr and retried every
  `RAT_TRIGGER`. Nothing is committed until a batch is processed. A 401
  triggers one re-login.
* A stream or topic that does not exist yet means idle, not an error. A topic
  created later is picked up from offset 0.
