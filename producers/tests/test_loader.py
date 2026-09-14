from rat_producers.app import App
from rat_producers.loader import load_all


def write_plugin(root, name, topic):
    d = root / name
    d.mkdir(parents=True)
    (d / "plugin.toml").write_text(
        f'name = "{name}"\n'
        f'topic = "{topic}"\n'
        f'interval = "60s"\n'
        f'\n'
        f'[payload]\n'
        f'schema = "schema.json"\n'
        f'\n'
        f'[entities]\n'
        f'from_fields = ["title"]\n'
    )


def test_later_root_overrides_same_name(tmp_path):
    repo, user = tmp_path / "repo", tmp_path / "user"
    write_plugin(repo, "hn", "events.hn")
    write_plugin(user, "hn", "events.user-hn")
    app = App()
    load_all(app, [repo, user])
    assert app.manifests["hn"]["topic"] == "events.user-hn"
    assert sorted(app.manifests) == ["hn"]


def test_dir_without_manifest_is_skipped(tmp_path):
    (tmp_path / "not-a-plugin").mkdir()
    app = App()
    load_all(app, [tmp_path])
    assert app.manifests == {}
