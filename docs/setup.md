# Setup

Nix is one way in. It is not required. Same versions either path.

## Required

| Tool | Version | What it is for |
|---|---|---|
| JDK | 17 | sbt, Spark, Metals |
| sbt | 1.10.11 | Spark job. Pinned in `spark/project/build.properties` |
| Python | 3.12 | producers |
| uv | 0.12+ | Python deps from `pyproject.toml` / `uv.lock` |

Optional until the cluster exists: Docker or Podman Compose, Metals, scala-cli.

`scripts/check-deps.sh` exits 0 when the required four are on `PATH` and Java is 17.

## Nix

```bash
direnv allow
```

or `nix develop`. The flake provides JDK 17, sbt, uv, Python 3.12, Metals. It runs `uv sync` on enter. Python packages still come from uv, not from Nix.

## Without Nix

Install the four tools, then:

```bash
uv sync --frozen
cd spark && sbt compile
```

`uv` reads `.python-version` (3.12) and will fetch that interpreter if the system one is older. Do not set `UV_PYTHON_DOWNLOADS=never` unless you already have 3.12.

### mise (easiest)

[mise](https://mise.jdx.dev/) pins the same versions in `mise.toml`.

```bash
curl https://mise.run | sh
mise install
eval "$(mise activate bash)"   # fish: mise activate fish
uv sync --frozen
```

With direnv, `.envrc` runs `use mise` when Nix is absent.

### Manual

**uv** (also gets you Python 3.12 via `uv sync`):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**JDK 17**, any Temurin or OpenJDK 17. `JAVA_HOME` must point at it.

- macOS: `brew install --cask temurin@17`
- Debian/Ubuntu: `sudo apt install openjdk-17-jdk`
- Fedora: `sudo dnf install java-17-openjdk-devel`
- Windows: Temurin 17, then WSL anyway (see below)

**sbt** 1.10.x. The project file will download 1.10.11 if the launcher is recent enough.

- macOS: `brew install sbt`
- Coursier: `cs install sbt:1.10.11`
- [sdkman](https://sdkman.io/): `sdk install sbt 1.10.11`

Then `uv sync --frozen` from the repo root.

## Editor

Metals for Scala (`spark/`). basedpyright or the Pyright plugin for `producers/`. On Nix, both LSPs are in the flake. Elsewhere, install Metals through the editor (it will use the JDK 17 on `PATH`).

## Windows

Use WSL2. Native Windows is a bad time for Spark, sbt, and the Hadoop-class cluster this job will talk to. Inside WSL, follow the non-Nix steps (or install Nix).
