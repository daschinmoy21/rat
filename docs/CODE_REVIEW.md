# Code review: producer delivery and stream correlation

Reviewed snapshot: [`017f22eecd004b595676b673dbc089844b0eb33c`](https://github.com/daschinmoy21/rat/commit/017f22eecd004b595676b673dbc089844b0eb33c)  
Date: 13 October 2026  
Scope: Python event extraction, delivery and cursors; Scala event parsing and correlation; related tests and CI.

This is an assisted static code review. Findings below are based on the referenced implementation. Runtime verification is listed separately; proposed tests are recommendations, not claims that tests have passed.

## Existing strengths

- Producer serialization is deterministic, and delivery waits for acknowledgement with `acks="all"`.
- Failed Kafka delivery has a DLQ path, a local spool fallback and broker backoff.
- Cursor queries use parameter binding, and cursor updates cannot move backwards.
- Correlation removes repeated entities within each envelope and uses `l.source < r.source` to avoid same-source and mirrored pairs.
- CI includes Python lint and tests, Scala formatting and tests, and Kafka/Hive smoke jobs.

## Findings

### 1. High: cursor advancement does not distinguish delivery from failed recovery

**Locations:** [cli.py](../producers/rat_producers/cli.py), `run`; [producer.py](../producers/rat_producers/producer.py), `emit`, `dlq`, `_spool`.

After `emit(env, producer)`, the loop always calls `cursor.put(name, env["ts_ms"])`. However, `emit` returns normally after falling back to the DLQ or spool, and `_spool` catches write failures and only logs them.

**Consequence:** if Kafka delivery and the spool both fail, the source cursor can advance past a lost event. Even a successful spool requires an explicit recovery process before that event reaches the primary stream.

**Recommendation:** return a delivery outcome distinguishing primary delivery, DLQ delivery, durable spooling and total failure. Define which outcomes may advance the cursor. Preserve the cursor after total failure and document spool replay, including duplicate handling.

**Suggested test:** simulate failed primary delivery, failed DLQ delivery and an unwritable spool; verify that `run` does not advance the source cursor. Separately test durable spool recovery.

### 2. Medium: malformed adapter output can abort the source poll before validation

**Locations:** [cli.py](../producers/rat_producers/cli.py), `run`; [test_producer.py](../producers/tests/test_producer.py), `test_dlq_survives_broken_envelopes`.

The loop assigns `env["entities"]` and performs extraction before calling `check`. An adapter returning `None` or a string raises before schema validation or DLQ routing. The exception handler surrounds the entire source poll, so subsequent rows from that poll are skipped.

**Consequence:** a malformed row can repeatedly block later valid rows, depending on the adapter's cursor and polling behavior. Robust handling inside `dlq` alone does not protect the ingestion loop.

**Recommendation:** validate the envelope's basic container shape before mutation. Handle invalid rows at the individual-row boundary and define a cursor policy for poison records. Retain source-level handling for poll failures.

**Suggested test:** return `[None, valid_envelope]` from a fake adapter and verify that the invalid row is quarantined and the valid row is processed.

### 3. Medium: entity deduplication does not prevent duplicate event correlations

**Locations:** [Correlate.scala](../spark/src/main/scala/rat/Correlate.scala), `perEntity` and `pairs`; [CorrelateSpec.scala](../spark/src/test/scala/rat/CorrelateSpec.scala).

`array_distinct` deduplicates entities within one envelope. It does not deduplicate repeated envelopes with the same `event_id`. The join also emits a row for each matching input-row combination.

**Consequence:** if one source event is replayed twice and matches one event from another source, the correlation can produce two equivalent rows. Checkpoints do not, by themselves, remove identical events stored at different Kafka offsets.

**Recommendation:** document whether replay duplicates are acceptable. If unique correlations are required, introduce bounded event deduplication with an explicit event identity and retention policy, or enforce uniqueness downstream.

**Suggested test:** duplicate an input envelope in the existing batch fixture, then add a streaming replay test for the chosen deduplication policy.

## Follow-up validation

The existing `CorrelateSpec` exercises the join with batch DataFrames and explicitly skips streaming watermarks. Add streaming checks for late arrivals, state cleanup and checkpoint restart before relying on those properties in deployment.

Use the repository's existing checks:

```bash
uv sync --frozen
uv run ruff check producers/
uv run pytest producers/tests/ -q
cd spark
sbt -batch test
```

Kafka and Hive integration checks are provided by `scripts/smoke.sh` and `scripts/smoke-hive.sh`.

## Checks run for this review

- `uv sync --frozen`: completed successfully.
- `uv run ruff check producers/`: all checks passed.
- `uv run pytest producers/tests/ -q`: 100 passed.
- Scala and Kafka/Hive smoke tests were not run in this review environment; `sbt` was unavailable.

These existing checks do not prove or resolve the findings above.

## Review boundary

This contribution documents findings and proposed improvements. It does not implement the proposed fixes or establish end-to-end delivery guarantees.

