# Getting started

`rat` is a single-machine event pipeline:

```text
Python plugins → Kafka → Spark event-time correlation → local Parquet
                                                    └→ optional HDFS/Hive
```

## Current status

Reviewed on 2026-09-25 against `main` commit `617663b` plus this branch.

- **Local MVP:** approximately 82% complete.
- **Demo readiness:** ready. The deterministic local pipeline and disposable HDFS/Hive pipeline pass in CI, and a live run on real data (all five plugins → Kafka → Spark → Parquet) was verified locally on 2026-09-25, including real `news` × `stocks` correlations.
- **Production readiness:** approximately 45–50%. Security, durable delivery, real-cluster validation, and release packaging are incomplete.
- **Latest main CI:** Python lint/tests, Scala format/tests, local end-to-end smoke, and HDFS/Hive smoke all passed in [run 35876185918](https://github.com/daschinmoy21/rat/actions/runs/35876185918).

### MVP progress

- [x] Kafka ingestion with Python plugin producers
- [x] HN, RSS, stocks, news (Yahoo Finance headlines), and Lobsters plugins
- [x] Live real-data run verified end to end
- [x] Spark Structured Streaming event-time correlation
- [x] Local Parquet output and query surface
- [x] HDFS and Hive external tables in CI
- [x] Local and HDFS/Hive smoke tests
- [ ] Persist every valid envelope, including entity-bearing events that do not correlate
- [ ] Prevent cursor advancement after an event is lost
- [ ] Make first startup safe when Spark uses its default `latest` offset
- [ ] Complete the documented plugin manifest and custom-root contracts
- [ ] Make the documented Nix container setup executable from a clean machine

## Known MVP blockers

There are no P0 failures: the checked-in happy path works. These P1 items still block a dependable local MVP.

| Blocker | User impact | Evidence |
|---|---|---|
| Incomplete container setup | The Nix shell has Podman and `docker-compose`, but the scripts and systemd units need either Docker + Compose or Podman + `podman-compose`. A clean Nix shell can therefore pass dependency checks and still fail to start Kafka. | [flake.nix](flake.nix#L21-L46), [scripts/smoke.sh](scripts/smoke.sh#L21-L33) |
| Unsafe first-start ordering | Spark defaults to `latest`. If producers publish before Spark establishes its first offsets, those records can be skipped permanently. | [RatConfig.scala](spark/src/main/scala/rat/RatConfig.scala#L46-L58), [cli.py](producers/rat_producers/cli.py#L61-L76) |
| Unmatched entity-bearing events disappear | `rat_events` stores only envelopes with no entities. An otherwise valid event with entities is retained only if it participates in a correlated pair, which does not satisfy the broad “a source shows up in Hive” goal. | [Correlate.scala](spark/src/main/scala/rat/Correlate.scala#L90-L99), [overview.md](docs/overview.md#L17) |
| Delivery can be lost before cursor commit | Producer failures fall through to DLQ/spool handling without returning an outcome, after which the runner advances the source cursor. A failed local spool can lose the row permanently. | [producer.py](producers/rat_producers/producer.py#L47-L116), [cli.py](producers/rat_producers/cli.py#L69-L76) |
| Plugin manifest contract is only partially enforced | Declared schema paths are ignored, a missing adapter is accepted, and a custom manifest topic is provisioned under one name but emitted under another. | [loader.py](producers/rat_producers/loader.py#L12-L43), [producer.py](producers/rat_producers/producer.py#L100-L116) |
| Custom plugin roots cannot be provisioned | Producers discover `RAT_PLUGINS_DIR` and user/system roots, while `make-topics.sh` only scans in-tree manifests. Kafka topic auto-creation is disabled. | [cli.py](producers/rat_producers/cli.py#L90-L109), [make-topics.sh](scripts/make-topics.sh#L28-L38) |

Lower-priority correctness gaps include global rather than per-plugin polling intervals, malformed adapter output bypassing the normal DLQ path, missing event-ID deduplication, and `rat status` reporting HDFS sink sizes as zero.

The first onboarding PR should make one container-engine path available and use it consistently in the smoke scripts, dependency checks, and systemd units. The first correctness PR should return an explicit delivery outcome and advance cursors only after Kafka delivery or durable quarantine/spooling. The current open issues do not track these blockers; #19 is deferred Parquet archival and #21 is stale dev-shell documentation.

## Prerequisites

Use Linux or WSL2.

Required:

- Git
- JDK 17
- Python 3.12 or newer
- uv 0.12 or newer
- sbt 1.10.11
- GNU `timeout`
- One complete container-engine pair:
  - Docker CLI plus Docker Compose v2, or
  - Podman plus `podman-compose`

The current Nix flake does not provide a complete supported pair. Install Docker with its Compose plugin or install `podman-compose` before running the local pipeline.

Default ports:

- Kafka: `9092`
- HDFS NameNode RPC/UI: `8020` / `9870`
- HDFS DataNode: `9866`
- Hive metastore / HiveServer2: `9083` / `10000`

## Install

```bash
git clone https://github.com/daschinmoy21/rat.git
cd rat
```

With Nix and direnv:

```bash
direnv allow
```

Without direnv, enter the shell manually:

```bash
nix develop
```

Install the locked Python environment and verify the checkout:

```bash
uv sync --frozen
./scripts/check-deps.sh
uv run rat plugins
(cd spark && sbt -batch compile)
```

`uv run rat plugins` should list `hn`, `lobsters`, `news`, `rss`, and `stocks` with their topics and intervals.

`check-deps.sh` validates Java and Python versions and checks that sbt and uv exist, but it does not validate sbt/uv versions, a container engine, Compose, or required ports.

## Fastest verified demo

The deterministic smoke test is the recommended first run. It does not call public APIs.

It:

1. Starts the in-tree Kafka broker.
2. Creates event and DLQ topics.
3. Emits a four-event fixture.
4. Runs `rat.Correlate` for at most seven minutes.
5. Queries Parquet and asserts the expected raw and correlated rows.

Run it from a clean shell without inherited `RAT_*` or `SPARK_MASTER` overrides:

```bash
./scripts/smoke.sh
```

Success ends with:

```text
SMOKE OK — raw row queryable, AAPL pair = one row, stocks pairs = two
```

Important details:

- A first SBT/Spark run can take several minutes.
- The script deletes and recreates `/tmp/rat-smoke`.
- It leaves Kafka running after success.
- It exercises a direct fixture producer, not live plugin polling.
- Never point `RAT_SINK_DIR` at durable data when using a smoke script.

Before a live run, reset Kafka as described in [Run on real data](#1-start-kafka-on-a-clean-volume), or the fixture events will replay into it.

Stop the broker after the demo:

```bash
# podman-compose installed (smoke.sh prefers it)
podman-compose -f infra/kafka/compose.yml down

# Docker only
docker compose -f infra/kafka/compose.yml down
```

Use one engine consistently. Do not mix `docker exec` and `podman exec` against the same container.

## Run on real data

This path polls the live public sources and runs producers, Spark, and queries as separate processes. It was verified end to end on 2026-09-25. No API keys are needed.

| Plugin | Source | Interval | Entities | Where its events land |
|---|---|---|---|---|
| `hn` | Hacker News top stories | 60s | `$TICKER` cashtags in title/url (rare) | mostly `rat_events` |
| `lobsters` | Lobsters hottest stories | 5m | `$TICKER` cashtags (rare) | mostly `rat_events` |
| `rss` | `hnrss.org/frontpage` by default | 5m | `$TICKER` cashtags (rare) | mostly `rat_events` |
| `stocks` | Yahoo Finance quote per watchlist symbol | 60s | the symbol | `correlated` when matched |
| `news` | Yahoo Finance headlines per watchlist symbol | 2m | the symbol | `correlated` when matched |

`news` and `stocks` share a watchlist (`AAPL MSFT GOOGL AMZN TSLA`) and are the pair that correlates on real data. HN, RSS, and Lobsters are entity-less unless a title contains a cashtag, so they show up in the raw table.

### 1. Start Kafka on a clean volume

The smoke test leaves its fixture events in Kafka. A live run that starts from `earliest` would replay them and pair them with real news, so reset Kafka's disposable volume first.

Use the engine that started the broker. `smoke.sh` uses Podman whenever `podman-compose` is installed and Docker otherwise. On a machine where `docker` is backed by Podman, `docker compose down` cannot see a container that `podman-compose` created and fails with `name "rat-kafka" is already in use`.

```bash
# podman-compose installed (what smoke.sh used):
podman-compose -f infra/kafka/compose.yml down -v
podman-compose -f infra/kafka/compose.yml up -d

# Docker only:
docker compose -f infra/kafka/compose.yml down -v
docker compose -f infra/kafka/compose.yml up -d
```

`-v` deletes all retained Kafka data. That is the point here, but never do it on a broker you care about.

The broker takes a few seconds to accept connections. If the next step fails with a connection error, wait and retry, or check `podman logs --tail 100 rat-kafka` (or `docker logs …`).

### 2. Create topics

Create them before Spark starts. Spark discovers new topics matching its pattern only every few minutes.

```bash
./scripts/make-topics.sh
```

Expected topics:

```text
events.hn          events.hn.dlq
events.lobsters    events.lobsters.dlq
events.news        events.news.dlq
events.rss         events.rss.dlq
events.stocks      events.stocks.dlq
```

The script is idempotent. It provisions only in-tree plugin manifests.

### 3. Start Spark (terminal 1)

```bash
rm -rf /tmp/rat-live /tmp/rat-live-cursors.db /tmp/rat-live-dlq

cd spark
RAT_SINK_DIR=/tmp/rat-live \
RAT_CHECKPOINT_DIR=/tmp/rat-live/checkpoints \
RAT_STARTING_OFFSETS=earliest \
RAT_TRIGGER="10 seconds" \
RAT_WATERMARK="24 hours" \
sbt -batch "runMain rat.Correlate"
```

Keep it running. The logs are noisy and all go to stderr. A `WARN StreamingJoinHelper: Error trying to extract state constraint ... Cannot evaluate expression: source#…` stack trace is expected and harmless: Spark logs it and continues. Real failures say `Query ... terminated with exception`, and then the process exits.

About the flags:

- `RAT_STARTING_OFFSETS=earliest` makes a fresh checkpoint read everything already in Kafka, so no events published before Spark is ready are lost.
- `RAT_WATERMARK` is the correlation window. The production default is `10 minutes`. Quotes only change while the US market is open (13:30–20:00 UTC, 19:00–01:30 IST). Outside those hours, `stocks` reports the last close, which is hours older than fresh headlines, so a 10-minute window finds nothing. Use `24 hours` for a demo at any time of day. During market hours, you can leave it at the default and still see pairs.

### 4. Run the producers (terminal 2)

From the repository root, poll every source once:

```bash
RAT_CURSOR_DB=/tmp/rat-live-cursors.db \
RAT_DLQ_SPOOL=/tmp/rat-live-dlq \
uv run rat run --once
```

This takes about 30 seconds and is silent unless a source fails. A first poll emits roughly 160 events: about 30 HN, 25 Lobsters, 20 RSS, 80 news, and 5 stocks.

To keep feeding data, run it continuously instead and stop it with Ctrl-C:

```bash
RAT_CURSOR_DB=/tmp/rat-live-cursors.db \
RAT_DLQ_SPOOL=/tmp/rat-live-dlq \
uv run rat run
```

Cursors stop repeat events: each source only emits items newer than its last emitted timestamp. The scheduler polls every source at the shortest interval across all manifests (currently 60s).

### 5. Check health (terminal 2)

```bash
RAT_CURSOR_DB=/tmp/rat-live-cursors.db \
RAT_SINK_DIR=/tmp/rat-live \
uv run rat status
```

Healthy output looks like this:

```text
broker   localhost:9092   up
source   cursor                   age    interval  state
hn       2026-09-25T10:59:08Z     37m    60s       ok
lobsters 2026-09-25T11:12:22Z     24m    5m        ok
news     2026-09-25T11:24:02Z     12m    2m        ok
rss      2026-09-25T10:59:08Z     37m    5m        ok
stocks   2026-09-24T20:00:01Z     15h    60s       ok
dlq      events.hn.dlq=0  events.lobsters.dlq=0  events.news.dlq=0  events.rss.dlq=0  events.stocks.dlq=0
sinks    correlated=…  events_raw=…
```

A 15h-old `stocks` cursor is normal outside market hours. Non-zero DLQ counts mean rows failed schema checks or delivery. Inspect them with:

```bash
RAT_CURSOR_DB=/tmp/rat-live-cursors.db uv run rat dlq --n 10
```

`rat status` does not prove Spark is healthy. Check terminal 1 for that.

### 6. Watch it in the dashboard (terminal 3)

```bash
RAT_SINK_DIR=/tmp/rat-live \
RAT_CURSOR_DB=/tmp/rat-live-cursors.db \
uv run rat dash
```

Open <http://127.0.0.1:8765/>. It shows the whole pipeline live: what Kafka carries, how far Spark has committed and what landed in Parquet. It binds to localhost only; use `--host` and `--port` to change that.

| View | Key | Shows |
|---|---|---|
| pipeline | `1` | Per source: message count, share of traffic, cursor age and DLQ count. Also Kafka throughput for the last 60s, each Spark query's last committed batch, and row counts and sizes for both Parquet sinks. |
| stream | `2` | Every envelope on `events.*`, read from the start of each topic and updated live. `Enter` opens the full JSON. `Space` pauses the feed. |
| correlated | `3` | Rows from the `correlated` Parquet sink. Each `a_id`/`b_id` is resolved to its headline or quote, and `Δt` is the gap between the two events. |
| stored | `4` | Rows from the `events_raw` Parquet sink. |
| dlq | `5` | Rejected envelopes and the `rat.error` reason. |

Every table view supports the same keys:

- `/` filters. Plain words match any column. `key:value` matches one column, e.g. `source:news` or `entity:tsla`. A leading `-` negates, e.g. `-hn`.
- `s` cycles the sort column and `r` reverses the order. Clicking a column header does the same.
- `g` cycles grouping. The stream view groups by source, entity, day or topic. The correlated view groups by entity, source pair or day. `Enter` on a group header folds it.
- `↑`/`↓` (or `j`/`k`) move the selection. `Esc` clears the filter, grouping and open rows.

The URL hash tracks the current view, so `http://127.0.0.1:8765/#correlated` links straight to it.

The dashboard holds the newest 5000 envelopes in memory (`--cap`). Pipeline counts still cover every message read. Parquet is re-read every 5 seconds with DuckDB. On NixOS, DuckDB needs `libstdc++`: the Nix dev shell sets `LD_LIBRARY_PATH` for it. Outside that shell, the pipeline view reports `duckdb unavailable` and the Kafka views keep working.

### 7. Query the results

Give Spark 30–60 seconds after the producers finish so the batch commits. Querying too early shows empty or partial tables. Then run:

```bash
cd spark
RAT_SINK_DIR=/tmp/rat-live sbt -batch "runMain rat.Query"
```

The query prints the 10 newest rows of each table. Output from the verified run (trimmed):

```text
== rat_events (raw, direct parquet) ==
|event_id                                         |entities|payload                                                   |
|lobsters:1ub0m3                                  |[]      |{"title":"AI is not “just a tool”", …}                    |
|rss:https://news.ycombinator.com/item?id=49842788|[]      |{"title":"Anthropic: The Situation Report", …}            |
|hn:49842596                                      |[]      |{"title":"Show HN: Agentic CUDA Kernel Optimizer", …}     |
== correlated (direct parquet) ==
|entity|a_id                                           |b_id                      |
|GOOGL |news:GOOGL:ea88cc38-1689-39e1-85c2-3624037927f6|stocks:GOOGL:1790280001000|
|TSLA  |news:TSLA:842a2c14-ad31-3009-a791-111edf8d80ef |stocks:TSLA:1790280000000 |
|TSLA  |news:TSLA:f20152d5-c38c-3d1c-80ad-574b31be1248 |stocks:TSLA:1790280000000 |
```

Which tickers pair depends on that day's headlines. The default `rss` feed is the HN front page, so some stories show up under both `hn:` and `rss:` IDs.

Each `correlated` row is a real headline (`a_id`) and a real quote (`b_id`) for the same ticker within the watermark window.

Things to know:

- `rat_events` holds only entity-less envelopes. An event with entities that never matches anything, such as a headline for a ticker with no quote in the window, is currently stored in neither table. This is a known MVP blocker.
- Tickers come only from `news`/`stocks` symbols and from `$CASHTAGS`. A plain "Apple" in an HN title does not become `AAPL`.
- Use `./scripts/smoke.sh` when you need an exact, repeatable result.

### Change the watchlist

To track other tickers, set the same list for both plugins:

```bash
mkdir -p ~/.config/rat
printf '[config]\nsymbols = ["NVDA", "AMD", "INTC"]\n' | tee ~/.config/rat/stocks.toml > ~/.config/rat/news.toml
```

RSS feeds can be overridden the same way in `~/.config/rat/rss.toml`. See [docs/plugins.md](docs/plugins.md#4-user-configuration-overrides).

## Optional HDFS and Hive demo

The HDFS/Hive smoke test starts Kafka, HDFS, Hive metastore, and HiveServer2, then queries both Hive tables.

> **Warning:** `scripts/smoke-hive.sh` deletes the entire HDFS `/rat` tree. Run it only against a disposable local stack with no durable data.

```bash
RAT_KEEP_CONTAINERS=true ./scripts/smoke-hive.sh
```

Success ends with:

```text
HDFS + HIVESERVER2 + BEELINE SMOKE OK
```

For manual HDFS/Hive operation, service configuration, checkpoints, and systemd usage, follow [docs/ops.md](docs/ops.md) and [deploy/README.md](deploy/README.md).

## Validation commands

Run the non-destructive unit checks from the repository root:

```bash
uv run ruff check producers/
uv run pytest producers/tests/ -q
(cd spark && scalafmt --test && sbt -batch test)
```

The stateful smoke commands are:

```bash
./scripts/smoke.sh
./scripts/smoke-hive.sh
```

The HDFS/Hive command is destructive to `/rat`; the local smoke command is destructive to its configured `/tmp` sink.

## Clean up the real-data run

Stop the producer and Spark processes first.

```bash
rm -rf -- /tmp/rat-live
rm -f -- /tmp/rat-live-cursors.db
rm -rf -- /tmp/rat-live-dlq
```

Stop Kafka with the same engine that started it:

```bash
docker compose -f infra/kafka/compose.yml down
# or: podman-compose -f infra/kafka/compose.yml down
```

Do not add `-v` unless you intentionally want to delete retained Kafka data.

## Configuration

| Variable | Purpose | Default |
|---|---|---|
| `RAT_BOOTSTRAP` | Kafka bootstrap servers | `localhost:9092` |
| `RAT_SINK_DIR` | Parquet root | `/tmp/rat` |
| `RAT_CHECKPOINT_DIR` | Spark checkpoint root | `$RAT_SINK_DIR/checkpoints` |
| `RAT_STARTING_OFFSETS` | Initial Kafka offsets for a new checkpoint | `latest` |
| `RAT_WATERMARK` | Join/event-time window | `10 minutes` |
| `RAT_TRIGGER` | Spark micro-batch interval | continuous |
| `RAT_CURSOR_DB` | Producer cursor database | `rat-cursors.db` |
| `RAT_DLQ_SPOOL` | Local fallback spool | `~/.local/state/rat/dlq-spool` |
| `SPARK_MASTER` | Spark master | `local[*]` |

Use `RAT_STARTING_OFFSETS=earliest` for a new disposable sink. Use `latest` only when records published before Spark starts are intentionally irrelevant.

## More documentation

- [Project overview](docs/overview.md)
- [Architecture and configuration](docs/architecture.md)
- [Setup](docs/setup.md)
- [Plugin development](docs/plugins.md)
- [Operations](docs/ops.md)
- [Deployment](deploy/README.md)
- [Contributing](CONTRIBUTING.md)
