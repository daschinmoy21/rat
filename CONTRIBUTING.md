# Contributing

Python source plugins, an Iggy broker and a Rust correlation/Parquet/SQL
engine. The Scala/Hadoop/Hive implementation remains as a legacy path.

## Shell

[docs/rust-migration.md](docs/rust-migration.md). Then:

```bash
direnv allow          # Nix
uv sync --frozen      # always
./scripts/check-deps.sh
cargo test --locked --workspace
cargo clippy --locked --all-targets -- -D warnings
```

Fix the machine if that fails.

## Where

| Change | Where |
|---|---|
| Source | plugin, see [docs/plugins.md](docs/plugins.md). Loaded by `rat_producers.loader` |
| Envelope | `producers/rat_producers/events.py`, `rust/rat-rs/src/envelope.rs`; retain legacy wire compatibility |
| Engine, Parquet, SQL | `rust/rat-rs/` |
| Rust dependency | `rust/rat-rs/Cargo.toml` and root `Cargo.lock` |
| Python lib | `uv add`, commit `uv.lock` |
| Legacy Spark lib | `spark/build.sbt` (3.5.3 / 2.13.14), `nix develop .#legacy` |
| Tool pins | `flake.nix` and `mise.toml` together |
| Topics | `plugins/*/plugin.toml` + `RAT_BROKER=iggy uv run rat topics` |
| Ops | `rat status` (broker, cursors, DLQ depth, sinks) · `rat dlq [--topic T] [--n N]` |
| Docs | `docs/` |

RSS **feed** = config. New **kind** of source (HN, stocks, IMAP) = plugin.

## Envelope

The JSON wire fields remain `event_id`, `source`, `entities`, `ts_ms`, and
`payload`. Payload stays plugin JSON. See [Iggy contract](docs/iggy-interface.md).

## Git

Do not commit `.venv/`, `target/`, `.env`, secrets. Branch off `main`, use small
PRs and report validation. Run `cargo fmt --all -- --check`, Rust tests/clippy,
`uv run ruff check producers/` and `uv run pytest producers/tests/` before
submitting. `scripts/smoke-rust.sh` exercises a real Iggy broker, restart
recovery and DataFusion queries; fake-server tests alone cannot verify the
published broker image.

Run `./scripts/iggy-up.sh`, then `RAT_BROKER=iggy uv run rat run --once` to
emit live events. Start `target/release/rat-rs consume` before emitting, or
use `RAT_STARTING_OFFSETS=earliest` with a fresh checkpoint. The original
Kafka/Spark smoke scripts and runbooks remain available for legacy users.
