"""JSON schemas and helpers for user activity events."""

import hashlib
import json

from jsonschema import validate, ValidationError  # noqa: F401 (re-exported)


USER_ACTIVITY_SCHEMA = {
    "type": "object",
    "properties": {
        "userId": {"type": "string", "minLength": 1},
        "activityType": {"type": "string", "minLength": 1},
        "timestamp": {"type": "string", "format": "date-time"},
    },
    "required": ["userId", "activityType", "timestamp"],
}

AGGREGATED_ACTIVITY_SCHEMA = {
    "type": "object",
    "properties": {
        "userId": {"type": "string", "minLength": 1},
        "date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
        "totalActivityCount": {"type": "integer", "minimum": 1},
    },
    "required": ["userId", "date", "totalActivityCount"],
    "additionalProperties": False,
}


def validate_user_activity_event(event_data: dict) -> None:
    """Raise ``ValidationError`` when *event_data* violates the input schema."""
    validate(instance=event_data, schema=USER_ACTIVITY_SCHEMA)


def validate_aggregated_activity_event(event_data: dict) -> None:
    """Raise ``ValidationError`` when *event_data* violates the output schema."""
    validate(instance=event_data, schema=AGGREGATED_ACTIVITY_SCHEMA)


def generate_event_id(user_id: str, activity_type: str, timestamp: str) -> str:
    """Produce a deterministic SHA-256 hash that uniquely identifies an event.

    The same logical event always yields the same id, which is the foundation
    of the idempotency mechanism.
    """
    canonical = json.dumps(
        {"userId": user_id, "activityType": activity_type, "timestamp": timestamp},
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
