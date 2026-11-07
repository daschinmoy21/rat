# Resource comparison: JVM stack and Rust port

Measured on 2026-11-06 on the same shared Linux VM: 4 vCPU (AMD EPYC 9645),
7.8 GiB RAM and 4 GiB swap. Initially no `rat` services or containers were
running, so the initial project footprint was zero. A stopped stack is not
a useful performance baseline: the original stack was started and measured
before the Rust stack was measured.

## Final results

| Metric | Legacy JVM stack | Rust + Iggy |
| --- | ---: | ---: |
| Mean working memory (MiB) | 2,905.8 | 163.3 |
| Peak working memory (MiB) | 3,193.5 | 185.5 |
| Mean swap (MiB) | 66.4 | 0.0 |
| Mean CPU (% of one core) | 82.2 | 8.3 |
| CPU seconds in 90-second window | 74.0 | 7.5 |
| Recorded block writes (MiB) | 210.8 | 87.1 |
| Acknowledged producer replay (seconds) | 87.8 | 21.9 |
| Parquet files | 505.0 | 24.0 |
| Parquet data (KiB) | 825.8 | 164.7 |

Mean working memory fell **94.4%** in this run; including swap, the reduction was **94.5%**. Average measured CPU fell **89.9%**. The independent row comparison matched every output row and its multiplicity: **7,500 correlations and 2,500 raw rows**, with no rejects, late drops, duplicate pairs, DLQ messages or spool output. See [parity-final.json](benchmarks/stack-2026-11-06/parity-final.json) and [consumer counters](benchmarks/stack-2026-11-06/rust-final-consumer.txt).

In separate 30-second settled-idle samples, working memory was 2,779.7 MiB legacy versus 163.4 MiB Rust; swap was 731.3 versus 0.0 MiB, and CPU was 16.7% versus 4.2% of one core.

The producer replay attempted exactly 10,000 rows on each backend, with broker/sink counts independently checked. Observed ingress was 113.9 versus 456.4 events/second. This includes the Python clients' synchronous per-row acknowledgement behavior (and Kafka's configured linger); it is **not** a broker maximum-throughput measurement or a comparable end-to-end latency result. The 90-second window includes idle time after Rust finishes and does not cover all of Spark's later backlog processing.

An on-demand DataFusion `query` over the final sinks completed in 28.5 ms with about 49.2 MiB peak RSS in a separate warm-cache sample. That query process is not resident and is excluded from the streaming-service totals.

The interactive [comparison chart](benchmarks/stack-2026-11-06/comparison.html) switches between working memory, working memory plus swap, and CPU.

## Runtime and build storage

- `docker.io/apache/kafka:3.7.0`: 378.5 MiB unpacked image. Multiple containers share an image.
- `docker.io/apache/hadoop:3.3.6`: 1,597.0 MiB unpacked image. Multiple containers share an image.
- `docker.io/apache/hive:3.1.3`: 853.2 MiB unpacked image. Multiple containers share an image.
- `docker.io/apache/iggy:0.9.0`: 125.3 MiB unpacked image. Multiple containers share an image.
- Rust release binary: 498.4 MiB, including line-table debug information. Debug information is optional deployment overhead; the measurement used this exact binary, whose SHA-256 is recorded in `implementation.json`.
- The legacy figures above do not include host-side Spark jars or JDK installation size. Build caches and development toolchains are separate from runtime service memory. The retained Rust build cache is about 6.6 GiB (debug plus release, including intermediate build profiles); it is not required in a deployment.

The Rust checkpoint was about 492.5 KiB after this workload. Spark checkpoint storage reported about 1,056 KiB by HDFS `du` (including its directory/metadata layout), so those checkpoint byte counts are indicative rather than identical accounting.

## Validation

- 40 Rust tests pass, including DataFusion, replay/recovery, watermark boundaries, malformed input, partial broker-response rollback, reset-path safety and live batch pacing. Full Clippy and rustfmt checks pass.
- 128 Python tests pass; Ruff is clean. New checks require persisted provisioning and reject weak/missing broker durability attestations.
- The real pinned Iggy image passed ingest → Parquet → SQL, consumer restart without duplication, and forced-kill recovery. All 10,000 persisted messages survived a hard broker restart; the same test with default replicated topics had lost all 10,000 before flush.
- The new smoke script repeats the broker crash check when it starts its own broker, including in CI; it leaves a shared pre-existing broker running.
- No actual power-loss or distributed failover test was performed.

## Method and limits

The legacy path used the committed Scala implementation, Kafka 3.7.0,
Hadoop 3.3.6 and Hive 3.1.3, with the repository's single-node compose
configuration. The Spark job ran directly on JDK 17 using sbt's exported
classpath and the Java options in `spark/build.sbt`. This excludes sbt's
additional resident JVM, so it is a conservative memory baseline relative
to `rat-spark.service`.

The Rust path uses the release build of `rat-rs`, Apache Iggy 0.9.0 pinned
by image digest, persisted topic durability, local Parquet, and DataFusion
54.1.0. No JVM service runs
on this path. DataFusion queries run on demand; there is no resident SQL
server equivalent to HiveServer2. Iggy was measured with an additional
empty setup stream as well as the isolated benchmark stream.

Both processors were configured with a 2-second micro-batch interval. Rust
honors a minimum interval between live polls/commits; finite file replay
runs without that delay. Batch boundaries can still differ. The original Spark default
of 200 shuffle partitions was retained. Rust Parquet and checkpoint output
were placed under `~/.local/state/rat/bench-20261106/` on ext4, the same host
filesystem backing the legacy container volumes. An earlier exploratory
Rust run used `/tmp` (tmpfs); its `rust-loaded.json` sample is retained for
transparency but excluded from the final disk-backed comparison.
An exploratory ext4 run (`rust-disk-loaded.json`) also used Iggy's default
replicated topic policy. It is excluded: a subsequent forced-kill test lost
all 10,000 buffered broker messages. A subsequent `rust-persisted-*.json` run
uses persisted topics and the producer requires the broker's persisted
attestation. A forced-kill canary verified restart survival; raw evidence
is in `iggy-persisted-canary.json`.
That run exposed excessive tiny batches (3,665 Parquet files and about
551 MiB of writes), which led to the live batch-interval fix. The final
comparison uses `rust-final-*.json` after that fix. All intermediate samples
remain available; they are not mixed into the final totals.
The fixture and Python `emit()` path were identical on both backends. This
replay bypasses external source polling and plugin schema validation, which
are covered separately by the plugin benchmarks. Six broker partitions per topic,
event topics and DLQs follow the existing plugin manifests. The fixture
contains 10,000 envelopes and should produce 7,500 correlations and 2,500
raw rows. Times fit inside one join window, so late-record policy cannot
affect this comparison. Each envelope repeats its entity, testing entity
deduplication within an envelope.

Metrics are cgroup v2 counters sampled once per second, using only the
named project containers and the processor's separate systemd scope.
Working memory is `memory.current - inactive_file`, not heap size or a
whole-host before/after delta. Swap is separate; add it when assessing
memory pressure. CPU 100% means one logical core, so the host has 400%
capacity. Block-I/O counters report actual storage activity, not logical
application data volume.

Python producers are excluded from the service-memory totals on both
paths; their acknowledged replay times are recorded separately, and their
process footprint is measured in the [plugin assessment](python-vs-lua.md).
The dashboard is also excluded on both paths. This is a modest synthetic
workload, not a maximum-throughput or sustained-production stress test.

The host runs unrelated services, and builds ran concurrently during parts
of the legacy measurements. They are excluded from the measured cgroups
but affect scheduling, cache pressure, swap and latency. These observations
support a large reduction in resident service memory on this local
architecture; they do not establish a universal throughput multiplier.

## Reproduction and evidence

The [measurement harness](../tools/stack-bench/README.md) contains commands
for both stacks, fixture generation, cgroup sampling, a finite-process
profiler and an independent Parquet verifier. Raw records, image digests,
environment details and test evidence are in
[benchmarks/stack-2026-11-06/](benchmarks/stack-2026-11-06/).

The verifier uses DuckDB only as an independent inspection tool; the Rust
application's SQL engine is DataFusion. It compares every sorted canonical
row, including multiplicity, while normalizing JSON object key order and
partition-date types. Matching counts alone would not prove parity.

## Scope of the replacement

This replaces a single-machine JVM deployment. Local storage does not
implement HDFS, and the SQL CLI does not implement Hive Thrift/JDBC. The
streaming watermark advances per record rather than per Spark micro-batch;
missing/null entities follow the documented raw-sink intent rather than
Spark's legacy `size(NULL) = -1` behavior. See
[migration](rust-migration.md) and [design details](rust-port.md) before
using it as a replacement for an existing distributed cluster.
