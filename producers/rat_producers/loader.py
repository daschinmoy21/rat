import importlib.util
import tomllib
from pathlib import Path


def load_plugin_dir(app, path: Path):
    manifest_path = path / "plugin.toml"
    if not manifest_path.exists():
        return
    manifest = tomllib.loads(manifest_path.read_text())
    adapter = path / "adapter.py"
    if adapter.exists():
        spec = importlib.util.spec_from_file_location(
            f"rat_plugin_{manifest['name']}", adapter)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.register(app)
    app.manifests[manifest["name"]] = manifest


def load_all(app, roots):
    for root in roots:
        for d in sorted(Path(root).glob("*/")):
            load_plugin_dir(app, d)
