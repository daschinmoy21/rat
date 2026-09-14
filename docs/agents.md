# Self-host and agents

The VPS runs rat. Hermes, Grok, and other CLI agents on that box treat it as a **tool**, not as a library they import.

One interface, three skins:

| Skin | Who uses it |
|---|---|
| `rat` CLI, JSON on stdout | Anything that can shell (Hermes, Grok, OpenCode, a human) |
| MCP stdio wrapping that CLI | Grok, and anything else that speaks MCP |
| Skill / tool prompt | Grok `SKILL.md`, Hermes equivalent: *when* to call `rat`, not a second API |

Do not build a Spark-aware agent. Do not give models JDBC to Hive as the first path. They get verbs: list sources, query entities, status, maybe add an RSS URL.

## CLI (the real API)

Stable, boring, JSON.

```
rat sources                 # loaded plugins + last event time
rat status                  # kafka, job, disk
rat query --entity AAPL --since 15m
rat events --source hn --limit 20
rat plugin list
```

Write path for agents that are allowed to mutate config:

```
rat rss add https://example.com/feed.xml
```

`--format json` is the default when stdout is not a TTY. Tables for humans. Agents should pass `--format json` anyway.

Exit non-zero on failure. No mixed log lines on stdout. Logs on stderr.

That is enough for a VPS agent. MCP is `rat` with a schema. A Grok skill is "run these commands, here is what the JSON means."

## MCP

One server, core-owned, stdio on the VPS:

```toml
# ~/.grok/config.toml (sketch)
[mcp_servers.rat]
command = "rat"
args = ["mcp"]
```

Tools map 1:1 to CLI verbs (`query`, `sources`, `status`, `events`). When a plugin registers `commands`, they show up as extra tools after reload. Agents discover `hn_top` because the HN plugin is loaded, not because MCP was rewritten.

Hermes or another agent that cannot do MCP still has the CLI.

## Skill

A short `SKILL.md` in the repo (`.grok/skills/rat/`) so Grok on the VPS knows:

- rat is the event bus for stocks / HN / RSS / …
- call `rat query` for "what is going on around X"
- call `rat sources` before assuming a plugin exists
- never SSH into Spark UI to answer a factual question

Other CLIs can grow a sibling prompt. The commands stay `rat`.

## What to run on the VPS

Self-hosted means the broker and the job live here, not Confluent Cloud.

Minimum that agents can actually query:

1. Kafka
2. Producer process (core + plugins)
3. Spark job writing parquet
4. A **query process** that reads those files (Hive if we already have it, or a small HTTP/CLI on parquet / DuckDB)

Hive is the course-shaped sink. It is a poor agent UX (beeline, fat JVM). Give agents `rat query` backed by whatever reads the parquet. Hive can stay for SQL humans.

Kafka + Spark + Hadoop-class Hive on a small VPS will hurt. Size the box for the JVMs or run Hive/Spark on a second host and keep `rat` CLI + Kafka + producers on the agent box. The CLI must be local to the agents. The cluster does not have to be.

Compose (later) should start the pipe with one command. Plugins mount in as a directory, so a new source is `git pull` or a copy into `/var/lib/rat/plugins`, then restart the producer, not a new image per source.

## Auth

Agents on the same VPS as `rat` can use a local socket or a loopback HTTP with a token in an env file. Do not bind query to `0.0.0.0` without that token. Plugin API keys (Polygon, IMAP) stay in `~/.config/rat/secrets` or env, never in plugin git.

## Order

1. CLI that can print `sources` / `status` even as stubs.
2. Kafka + one plugin (RSS is the cheapest real one).
3. Spark writing parquet, `rat query` reading it.
4. MCP + skill wrapping the same CLI.
5. More plugins (HN, stocks). Newsletters through RSS until they cannot.
