# Contributing

Group repo. Nobody is assigned a lane here. The layout is the contract: producers in Python, the streaming job in Scala, Hive is query-only.

## Get a shell

[docs/setup.md](docs/setup.md) is the install page. Short version:

```bash
# Nix
direnv allow

# not Nix: mise install, or JDK 17 + sbt + uv by hand
uv sync --frozen
./scripts/check-deps.sh
```

`check-deps.sh` wants JDK 17, sbt, uv, Python 3.12+. If that fails, fix the machine before touching code.

## Where code goes

| Change | Directory | Tool |
|---|---|---|
| New source / producer | `producers/rat_producers/` | uv, Python 3.12 |
| Event fields | Python `events.py` **and** Scala `Event.scala` | both |
| Windows, joins, sink | `spark/src/main/scala/rat/` | sbt |
| Python libraries | repo root `pyproject.toml` | `uv add`, commit `uv.lock` |
| Spark libraries | `spark/build.sbt` | sbt |
| JDK / sbt / Python / uv pins | `flake.nix` **and** `mise.toml` | keep them in sync |
| Why / architecture / setup | `docs/` | markdown |

Do not put PySpark in `spark/`. Do not have a producer write to Hive. Do not add a second event schema.

## Adding a source

1. New module under `producers/rat_producers/`. Name it after the source (`host.py`, `svc.py`, whatever it actually is).
2. Emit `Event` from `events.py`. Set `source`, `entity_id`, `ts_ms`. If you cannot name an entity, it will not join later.
3. One Kafka topic per source, same name as `source` unless you have a reason not to.
4. Leave `Correlate.scala` alone unless the job must learn a new field or a new join.

## Changing the event shape

Two files, same meaning:

- `producers/rat_producers/events.py`
- `spark/src/main/scala/rat/Event.scala`

Python uses `event_id`, `ts_ms`. Scala uses `eventId`, `tsMs`. Same fields. If you add one, add it on both sides in the same change.

## Deps

Python:

```bash
uv add some-lib
uv remove some-lib
```

Commit `pyproject.toml` and `uv.lock`. Do not pip-install into the void. Do not add Python packages to `flake.nix`.

Scala: edit `spark/build.sbt`. Spark stays `3.5.3` / Scala `2.13.14` unless the group decides to bump both. `%%` already pulls `_2.13` artifacts.

After a Python dep change, `uv sync --frozen` must work on a clean tree. After a Scala dep change, `cd spark && sbt compile` must work.

## What not to commit

`.venv/`, `spark/target/`, `.metals/`, `.bloop/`, `.direnv/`, `__pycache__/`, `.env`, broker data, Hive warehouses. `.gitignore` already covers the usual ones.

Secrets stay out of git. Kafka passwords, API keys, hostnames that should not be public: env vars or a local file that is gitignored.

## Branches and PRs

`main` is the default branch. Work on a short branch (`producers-host`, `spark-watermark`, `docs-setup`). Open a PR into `main`.

Say what you touched and how you checked it (`uv sync`, `sbt compile`, `./scripts/check-deps.sh`). Small diffs. Do not mix a producer, a Spark rewrite, and a flake bump in one PR unless they are actually one change.

## Status of the repo

Scaffold. `Correlate` starts a local SparkSession and stops. There is no Kafka compose in-tree yet. If you add the broker, put compose and notes under something obvious (`compose/`, `docs/`) and point the README at it.
