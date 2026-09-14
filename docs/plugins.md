# Plugins

Core does not know HN, stocks, or RSS. A plugin is a directory: manifest, poller, payload schema. Drop it in, restart, new source.

Spark reads the envelope only. If adding a feed needs a Scala rebuild, the split failed.

## Core

Envelope, Kafka (`events.<source>` + DLQ), scheduler, cursor store (SQLite), config (`/etc/rat`, `~/.config/rat`), Spark job, `rat` CLI.

Not: tickers, HN JSON, RSS guids, IMAP.

## Plugin

```
plugins/hn/
  plugin.toml     required
  adapter.py      poll / register
  schema.json     payload
  extract.py      optional
```

Also `/var/lib/rat/plugins/` and `~/.config/rat/plugins/`. Load order: repo, machine, user. Later name wins.

```toml
name = "hn"
topic = "events.hn"
interval = "60s"

[payload]
schema = "schema.json"

[entities]
from_fields = ["title", "url"]
```

```python
def register(app):
    app.add_source("hn", poll=poll)
    app.add_extractor("hn", cashtags)
```

`poll()` returns envelopes. No entities → still emit, Spark skips the join.

| Plugin | Core |
|---|---|
| payload schema | envelope |
| poll / stream | Kafka client |
| entity extract | windows, watermarks |
| interval, topic | Hive layout |
| extra `rat hn …` | |

New RSS feed = config. New kind (HN, stocks, IMAP) = plugin. Substack is RSS. IMAP only if there is no feed.

## Envelope vs payload

```
event_id     hn:12345678
source       hn
entities     ["AAPL"]
ts_ms        …
payload      { "title": "...", "score": 42 }
```

`events.py` / `Event.scala` are the envelope. Payload is `dict` / `String`.

Hooks: `poll`, `extract`, `commands`. Stop there. Correlation is entity + time.
