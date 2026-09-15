import json

import pytest

from rat_producers.schema import (
    check,
    envelope_problems,
    load_validator,
    payload_problems,
)


def envelope(**overrides):
    env = {
        "event_id": "hn:1",
        "source": "hn",
        "entities": ["AAPL"],
        "ts_ms": 1725200000000,
        "payload": {"title": "thing"},
    }
    env.update(overrides)
    return env


def test_clean_envelope_has_no_problems():
    assert envelope_problems(envelope()) == []


def test_non_object_envelope():
    assert envelope_problems(None) == ["envelope is not an object"]
    assert envelope_problems("nope") == ["envelope is not an object"]


def test_missing_or_wrong_typed_fields():
    for env, needle in [
        (envelope(event_id=""), "event_id"),
        (envelope(event_id=7), "event_id"),
        (envelope(source=None), "source"),
        (envelope(entities="AAPL"), "entities"),
        (envelope(entities=[1]), "entities"),
        (envelope(ts_ms="soon"), "ts_ms"),
        (envelope(ts_ms=True), "ts_ms"),  # bool is an int subclass; still wrong
        (envelope(payload=None), "payload"),
        (envelope(payload=None), "object"),
    ]:
        assert any(needle in p for p in envelope_problems(env)), env


def test_payload_problems_against_plugin_schema():
    schema = {
        "type": "object",
        "required": ["title"],
        "properties": {"score": {"type": "integer"}},
    }
    validator = load_validator_json(schema)
    assert payload_problems({"title": "t"}, validator) == []
    problems = payload_problems({"score": "high"}, validator)
    assert len(problems) == 2  # missing title + score not an integer
    assert any("not of type 'integer'" in p for p in problems)


def test_no_validator_means_no_payload_check():
    assert payload_problems({"anything": 1}, None) == []


def test_check_skips_payload_when_envelope_broken():
    schema = {"type": "object", "required": ["title"]}
    validator = load_validator_json(schema)
    problems = check(envelope(payload={"score": 1}, ts_ms="bad"), validator)
    assert problems == ["ts_ms not an integer"]


def test_check_reports_schema_problems():
    schema = {"type": "object", "required": ["title"]}
    problems = check(envelope(payload={}), load_validator_json(schema))
    assert problems == ["'title' is a required property"]


def load_validator_json(schema):
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as d:
        p = Path(d)
        (p / "schema.json").write_text(json.dumps(schema))
        return load_validator(p)


def test_load_validator_reads_real_plugin_schemas():
    from pathlib import Path

    hn = load_validator(Path(__file__).parents[2] / "plugins" / "hn")
    assert hn.is_valid({"title": "t", "score": 5})
    assert not hn.is_valid({"score": "lots"})


def test_load_validator_rejects_broken_schema(tmp_path):
    (tmp_path / "schema.json").write_text("{not json")
    with pytest.raises(json.JSONDecodeError):
        load_validator(tmp_path)
