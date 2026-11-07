# RAT — Recorded project demo script

**Presenters:** three people. Replace `[Name 1]`, `[Name 2]`, and `[Name 3]` before recording.

**Target length:** 12–13 minutes, including screen actions and handoffs. **Hard limit:** 15 minutes.

*Italic text contains screen directions and is not spoken.*

## Before recording

- Start Kafka, Spark, and the dashboard using the real-data instructions in [GETSTARTED.md](../GETSTARTED.md#run-on-real-data).
- Keep the dashboard populated, with one clear news–stock correlation ready to inspect.
- Use the same sink and cursor paths in all demo terminals. Commands below assume `/tmp/rat-live` and `/tmp/rat-live-cursors.db`.
- Prepare tabs for the architecture diagram, a plugin directory, the dashboard, and query output.
- If using the documented 24-hour correlation range for an off-market demo, say so. Do not describe it as the default.
- Cut long command waits during editing. Label fixture data or previously recorded output if used.
- The agent section explains an architectural capability. Only claim an agent built a demonstrated plugin if that actually happened.

## Person 1 — Problem, architecture, and agent angle

**Time: 0:00–4:00**

*Show the project title and team names.*

“Hello everyone. In this video, we’ll demonstrate our project, RAT—a streaming pipeline that collects data from multiple sources, connects related events, and stores the results for querying.

I’m [Name 1]. I’ll introduce the architecture and explain how it supports agent-built integrations. [Name 2] will demonstrate the plugin and ingestion layer, and [Name 3] will show processing and the final results.

The problem we’re addressing is that related information arrives through separate systems and in different formats.

For example, a financial news feed might publish a headline about Apple, while a stock data source provides an Apple stock quote. These are different records, but they share a useful identifier: the stock ticker, AAPL.

We want to collect both, connect them using that shared entity and their timestamps, and make the result available for analysis.”

*Show the architecture diagram in `docs/architecture.md`.*

“The pipeline has four main stages.

Python plugins fetch data and convert it into a common event format. Kafka receives and buffers those events. Spark Structured Streaming reads the events and correlates records using entities and event time. Finally, Spark writes Parquet output, which can be queried locally or exposed through Hive when using the HDFS and Hive setup.

For this recording, we’re showing the local pipeline and its dashboard.”

*Show an existing plugin directory beside the architecture.*

“The plugin boundary is also where coding agents fit into the project.

An agent can inspect the plugin contract and existing examples, then implement a new source adapter. Its task is to produce a manifest, a payload schema, and Python code that returns events in the expected format.

After validation and topic provisioning, that source can use the existing pipeline without adding source-specific logic to Spark.

This makes adding a source a bounded task that a human or coding agent can carry out and test.”

*Open an event in the dashboard’s stream view.*

“Every source uses the same event envelope: an event ID, a source name, entities, an event timestamp, and a payload.

The payload preserves source-specific information. The shared fields let the rest of the pipeline process different sources consistently.

One important distinction: our correlation identifies records sharing an entity within a time range. A headline paired with a stock quote does not prove that the headline caused a price movement.

[Name 2] will now show the plugin contract and how data enters the pipeline.”

## Person 2 — Plugin contract, agent workflow, and ingestion

**Time: 4:00–8:00**

*From the repository root, run:*

```bash
uv run rat plugins
```

“The project currently includes five plugins: Hacker News, RSS, Lobsters, financial news, and stock quotes.

This command lists the discovered plugins, their topics, and their configured intervals.”

*Open `plugins/stocks/plugin.toml`, then `schema.json` and `adapter.py`.*

“These three files define the integration boundary.

The manifest declares the plugin and its configuration. The JSON schema defines the expected payload. The adapter handles fetching data and returning event envelopes.

The core provides shared behavior such as validation, entity extraction, and Kafka publishing.

This structure gives a coding agent a concrete implementation task. For example, we could ask:

‘Using the existing plugins as examples, build a plugin for this API. Follow the envelope contract, define the payload schema, extract appropriate entities, and add tests with mocked responses.’

The agent would still need to handle the source’s API details, rate limits, pagination, and timestamps. We would review and test the generated plugin before using it.”

*Point to entity configuration, then show a news event and a stock event.*

“Choosing consistent entities is especially important.

Our news and stock plugins both use ticker symbols such as AAPL. That gives Spark a shared key.

For a future cryptocurrency integration, a price plugin and a news plugin would need to agree on an identifier such as BTC. Following the envelope makes the records compatible with the pipeline; choosing consistent identifiers makes correlation useful.

Our current default extractor for text sources recognizes cashtags such as dollar-sign AAPL. It does not automatically convert every company name into a ticker.”

*Switch to the dashboard’s pipeline view, then start one collection cycle in a prepared terminal:*

```bash
RAT_CURSOR_DB=/tmp/rat-live-cursors.db \
RAT_DLQ_SPOOL=/tmp/rat-live-dlq \
uv run rat run --once
```

“This polls each source once. Since we started the pipeline before recording, there are already records available to inspect while collection runs.

The producers keep source cursors in SQLite to track collection progress and help adapters skip older items on later polls.”

*Open the stream view and expand a record.*

“Here we can inspect the exact envelope published to Kafka, including the source, timestamp, entities, and original payload.

Payloads that fail schema validation are routed to a dead-letter topic with an error reason. The dashboard has a separate view for inspecting those records.

The pipeline view also shows traffic, cursor information, Spark progress, and output storage, letting us follow the data across stages.

I’ll hand over to [Name 3] to show how these inputs become correlated results.”

## Person 3 — Correlation, querying, limitations, and conclusion

**Time: 8:00–12:30**

*Open the correlated view and select a clear news–stock pair.*

“This view shows the main processing result.

Each row represents a pair of events that matched on an entity and met the configured time condition. Here, a financial news event and a stock quote share the same ticker.”

*Point to the entity, event IDs, resolved records, and time difference.*

“The entity explains the matching key. The event IDs let us trace the pair to its inputs, and the time difference shows how far apart the events occurred.

Spark uses event time—the timestamp inside each record—rather than only the time it receives the record. This matters because sources can deliver events with different delays.

The job also uses a watermark to manage late data and limit retained state.

The default correlation range is ten minutes. For an off-market demonstration, the documented configuration uses a wider range because the latest stock quote may be older than recently collected headlines.”

*Expand the pair to show the actual headline and quote. Pause briefly so viewers can read it.*

“We can inspect the actual records here, so the result is traceable beyond just a count of matches.”

*Switch to the stored view.*

“Events without extracted entities follow the raw storage path. Many Hacker News, RSS, and Lobsters records appear here.

There is a current limitation: an event with entities that never finds a match is absent from both output tables. Preserving every valid event is one of our next improvements.”

*Show prepared query output, or run this command from the repository root and cut the startup wait:*

```bash
cd spark
RAT_SINK_DIR=/tmp/rat-live sbt -batch "runMain rat.Query"
```

“These results are stored in Parquet, so they remain available after processing. This command shows the raw and correlated outputs.

In local mode, it reads the files directly. With the HDFS and Hive configuration, HDFS provides storage and Hive exposes external tables over the output files.”

*Point to an example row from each output.*

“The repository also includes unit tests and deterministic end-to-end smoke tests. Fixed input events let us check expected results independently of changing public APIs.

Further work includes retaining unmatched events, strengthening delivery guarantees, and validating operation on a larger cluster.”

*Return to the architecture or closing slide.*

“In this recording, we demonstrated collection through Python plugins, transport through Kafka, correlation through Spark, and inspection of stored results.

The broader idea is that the plugin contract gives coding agents a repeatable way to extend the system. An agent can implement and test the source-specific adapter, while the existing pipeline provides shared processing and storage.

Our demo shows the architecture that enables that workflow. A next demonstration would follow an agent-created plugin all the way from implementation to queryable results.

Thank you for watching.”

## Final recording checklist

- Replace presenter names and rehearse each section near four minutes.
- Keep the complete edited recording below 15 minutes; target 12:30.
- Use existing output if a fresh poll returns no new records.
- Keep a screenshot or recording of a clear correlated pair as a fallback.
- Match the narration to the configuration and output actually shown.
- If demonstrating a genuinely agent-built plugin, replace the final “next demonstration” sentence with a specific statement about what was generated, reviewed, and verified.
