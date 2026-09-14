# Core and plugins

Emacs-shaped, not Emacs. The core does not know Hacker News, stocks, or RSS. A plugin registers a source, its payload schema, how to poll it, and how to name entities. Drop a directory in, restart the producer, the bus has a new source.

Spark stays generic. It reads the envelope, not plugin classes. If adding a feed means recompiling Scala, the split failed.

## Core

The core owns:

- The **envelope**: `event_id`, `source`, `entities`, `ts_ms`, `payload` (JSON object, plugin-defined).
- Kafka produce / consume helpers, topic naming (`events.<source>`), a DLQ.
- A scheduler. Plugins say how often. Core sleeps and calls them.
- A cursor store (SQLite is enough) so plugins can remember etags and last ids.
- Config merge: repo defaults, then `/etc/rat/`, then `~/.config/rat/`.
- The **tool surface** agents use: CLI, and MCP wrapping that CLI. Plugins do not speak MCP themselves.
- The Spark job: watermark, explode `entities`, window join, parquet, Hive (or a thin query API on that parquet).

Core does not own ticker lists, HN item JSON, RSS `guid` mapping, or IMAP.

## Plugin

A plugin is a directory on the load path:

```
plugins/hn/
  plugin.toml      manifest, required
  adapter.py       poll() / register()
  schema.json      payload shape, required
  extract.py       optional, entities from payload
```

User / VPS extras (Emacs `~/.emacs.d`):

```
/var/lib/rat/plugins/          # machine
~/.config/rat/plugins/         # person
```

Load path, in order: repo `plugins/`, then machine, then user. Same name later in the path wins.

`plugin.toml` is the autoload file:

```toml
name = "hn"
topic = "events.hn"
interval = "60s"
language = "python"

[payload]
schema = "schema.json"

[entities]
from_fields = ["title", "url"]
```

Python side is a hook, not a base class pyramid:

```python
def register(app):
    app.add_source("hn", poll=poll)
    app.add_extractor("hn", cashtags)
```

`poll()` returns envelopes. Core assigns nothing about HN. If `poll` cannot name entities, it still emits; Spark lands raw and skips the join for those rows.

### What belongs in a plugin

| In the plugin | Not in the plugin |
|---|---|
| Payload schema | Envelope fields |
| Poll / webhook / websocket | Kafka client setup |
| Entity extraction for that source | Window size, watermarks |
| Default interval, topic name | Hive table layout |
| Extra CLI verbs (`rat hn top`) | MCP server, auth for agents |

Stocks, HN, RSS, newsletters are four plugins. RSS should eat most newsletters (Substack is a feed). IMAP is a fifth plugin only if a source has no feed.

Adding a feed to RSS is **config**, not a new plugin. Adding Polygon instead of a Yahoo poller is a new plugin, or a second adapter inside `stocks/`.

## Schema split

Envelope is core and frozen-ish. Payload is plugin and can be ugly.

```
event_id     hn:12345678
source       hn
entities     ["AAPL"]
ts_ms         …
payload      { "title": "...", "score": 42, ... }   # schema.json for hn
```

Spark and Hive see the envelope plus `payload` as JSON. A plugin that needs a typed Hive view can ship a `.hql` snippet. Core does not generate a new Scala case class per plugin.

Python `events.py` and Scala `Event.scala` stay the envelope only. Payload stays `dict` / `String`.

## Hooks (the Emacs part)

Small list. Do not grow it until something hurts.

- `poll` → list of envelopes
- `extract` → entities from a payload
- `commands` → extra `rat <plugin> …` subcommands
- later, if needed: `on_cursor`, `map_event`

No `on_every_spark_stage`. Correlation is envelope + time. Custom joins wait until a real one exists.

## Agents and self-host

Plugins are how sources appear. Agents never import a plugin. They call core.

See [agents.md](agents.md).
