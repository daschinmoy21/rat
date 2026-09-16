"""Envelope and payload validation. Bad rows go to the DLQ, never the bus."""

import json
from pathlib import Path

import jsonschema


def load_validator(plugin_dir: Path) -> jsonschema.Draft7Validator:
    """Validator for a plugin's payload schema (schema.json next to plugin.toml)."""
    schema = json.loads((plugin_dir / "schema.json").read_text())
    cls = jsonschema.validators.validator_for(schema)
    cls.check_schema(schema)
    return cls(schema)


def envelope_problems(env) -> list[str]:
    """Shape problems with the envelope itself, empty list when it is clean."""
    if not isinstance(env, dict):
        return ["envelope is not an object"]
    problems = []
    if not isinstance(env.get("event_id"), str) or not env["event_id"]:
        problems.append("event_id missing or not a string")
    if not isinstance(env.get("source"), str) or not env["source"]:
        problems.append("source missing or not a string")
    entities = env.get("entities")
    if not isinstance(entities, list) or not all(isinstance(e, str) for e in entities):
        problems.append("entities not a list of strings")
    ts = env.get("ts_ms")
    if not isinstance(ts, int) or isinstance(ts, bool):
        problems.append("ts_ms not an integer")
    if not isinstance(env.get("payload"), dict):
        problems.append("payload not an object")
    return problems


def payload_problems(payload, validator) -> list[str]:
    """Payload problems against the plugin's schema.json, empty when clean."""
    if validator is None:
        return []
    return [err.message for err in validator.iter_errors(payload)]


def check(env, validator) -> list[str]:
    """All problems with one envelope: shape first, then payload schema."""
    problems = envelope_problems(env)
    if not problems and isinstance(env, dict):
        problems.extend(payload_problems(env.get("payload"), validator))
    return problems
