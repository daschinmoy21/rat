# Setup

JDK 17, sbt 1.10.11, Python 3.12, uv 0.12+. `./scripts/check-deps.sh` checks that.

## Broker + topics

```bash
podman-compose -f infra/kafka/compose.yml up -d   # single-node KRaft, localhost:9092
./scripts/make-topics.sh                          # events.* + DLQs from plugin manifests
```

Topic creation is not automatic (`auto.create.topics.enable=false` on purpose): new source = plugin + one `make-topics.sh` run. Idempotent; re-run any time.

Running the whole pipe as a system (services, verification, DLQ, checkpoints): [ops.md](ops.md).

The Nix shell includes Podman, Docker Compose, Metals, scala-cli, coursier, and scalafmt.

## Nix

```bash
direnv allow    # or nix develop
```

The flake supplies the toolchain, container runtime, Compose, and Scala tooling. `uv sync` still installs Python packages.

## Not Nix

```bash
uv sync --frozen
cd spark && sbt compile
```

uv reads `.python-version` (3.12) and will fetch it.

**mise:** [mise](https://mise.jdx.dev/) + `mise.toml`.

```bash
curl https://mise.run | sh
mise install
eval "$(mise activate bash)"   # fish: mise activate fish
uv sync --frozen
```

direnv uses mise when Nix is missing.

**Manual:** [uv](https://docs.astral.sh/uv/), Temurin/OpenJDK 17 (`JAVA_HOME`), sbt 1.10.x (`brew install sbt`, `cs install sbt:1.10.11`, or sdkman). Then `uv sync --frozen`.

Windows: WSL2. Native Windows is a bad time for this stack.
