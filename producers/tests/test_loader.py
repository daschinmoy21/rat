import pytest

from rat_producers.app import App
from rat_producers.loader import load_all


def write_plugin(root, name, topic, schema=True, interval="60s"):
    d = root / name
    d.mkdir(parents=True)
    (d / "plugin.toml").write_text(
        f'name = "{name}"\n'
        f'topic = "{topic}"\n'
        f'interval = "{interval}"\n'
        f'\n'
        f'[payload]\n'
        f'schema = "schema.json"\n'
        f'\n'
        f'[entities]\n'
        f'from_fields = ["title"]\n'
    )
    if schema:
        (d / "schema.json").write_text('{"type": "object"}')


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


def test_topic_must_look_like_events_dot_source(tmp_path):
    write_plugin(tmp_path, "hn", "hn-events")
    with pytest.raises(ValueError, match="topic"):
        load_all(App(), [tmp_path])


def test_topic_charset_is_constrained(tmp_path):
    write_plugin(tmp_path, "hn", "events.hn..;rm")
    with pytest.raises(ValueError, match="topic"):
        load_all(App(), [tmp_path])


def test_interval_format_is_checked(tmp_path):
    write_plugin(tmp_path, "hn", "events.hn", interval="hourly")
    with pytest.raises(ValueError, match="interval"):
        load_all(App(), [tmp_path])


def test_schema_json_is_required(tmp_path):
    write_plugin(tmp_path, "hn", "events.hn", schema=False)
    with pytest.raises(ValueError, match="schema.json"):
        load_all(App(), [tmp_path])
