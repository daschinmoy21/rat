{
  description = "Kafka → Spark Structured Streaming → Hive/HDFS (Python producers, Scala job)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        };
        # Spark 3.5 and Metals. Hive 3 in compose can stay on 11; this shell is the job.
        java = pkgs.jdk17;
        python = pkgs.python312;
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            java
            sbt
            scala-cli
            metals
            coursier
            scalafmt

            python
            uv
            basedpyright

            podman
            docker-compose

            nixd
            yaml-language-server
            bash-language-server
          ];

          shellHook = ''
            export JAVA_HOME="${java.home}"
            export PATH="$JAVA_HOME/bin:$PATH"
            export SBT_OPTS="''${SBT_OPTS:--Xmx2G -Xms512M}"
            export UV_PYTHON="${python}/bin/python"
            export UV_PYTHON_DOWNLOADS=never
            if [ -S "/run/user/$(id -u)/podman/podman.sock" ]; then
              export DOCKER_HOST="unix:///run/user/$(id -u)/podman/podman.sock"
            fi
            if [ -f pyproject.toml ]; then
              uv sync --frozen || uv sync
            fi
            if [ -d .venv/bin ]; then
              export VIRTUAL_ENV="$PWD/.venv"
              export PATH="$VIRTUAL_ENV/bin:$PATH"
            fi
            echo "rat  producers=python  spark=scala"
            echo "java $(java -version 2>&1 | head -n1)"
            echo "uv $(uv --version)  python $(python --version 2>/dev/null | awk '{print $2}')"
          '';
        };
      });
}
