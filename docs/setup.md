# Setup

JDK 17, sbt 1.10.11, Python 3.12, uv 0.12+. `./scripts/check-deps.sh` checks that.

Compose / Metals / scala-cli later, when the cluster exists.

## Nix

```bash
direnv allow    # or nix develop
```

Flake gives the four tools. `uv sync` still installs Python packages.

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
