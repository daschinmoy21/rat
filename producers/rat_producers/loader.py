import importlib.util
import re
import tomllib
from pathlib import Path

from rat_producers.schema import load_validator

_TOPIC = re.compile(r"^events\.[a-z0-9_-]+$")
_INTERVAL = re.compile(r"^\d+[smh]$")


def _validate_manifest(manifest, path: Path):
    name = manifest.get("name")
    if not name or not isinstance(name, str):
        raise ValueError(f"{path}: manifest needs a name")
    topic = manifest.get("topic")
    if not topic or not _TOPIC.match(topic):
        raise ValueError(f"{path}: topic {topic!r} must look like events.<source>")
    interval = manifest.get("interval")
    if not interval or not _INTERVAL.match(str(interval)):
        raise ValueError(f"{path}: interval {interval!r} must look like 60s/5m/1h")
    if not (path / "schema.json").exists():
        raise ValueError(f"{path}: schema.json is required")


def load_plugin_dir(app, path: Path):
    manifest_path = path / "plugin.toml"
    if not manifest_path.exists():
        return
    manifest = tomllib.loads(manifest_path.read_text())
    _validate_manifest(manifest, path)
    # schema before register: a broken schema.json must fail before the
    # plugin's source is attached, leaving no half-loaded plugin behind
    validator = load_validator(path)
    adapter = path / "adapter.py"
    if adapter.exists():
        spec = importlib.util.spec_from_file_location(
            f"rat_plugin_{manifest['name']}", adapter)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.register(app)
    app.manifests[manifest["name"]] = manifest
    app.schemas[manifest["name"]] = validator


def load_all(app, roots):
    for root in roots:
        for d in sorted(Path(root).glob("*/")):
            load_plugin_dir(app, d)
