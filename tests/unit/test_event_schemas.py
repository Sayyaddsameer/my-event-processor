"""Unit tests for event schema validation and helper utilities."""

import pytest
from jsonschema import ValidationError

from src.models.event_schemas import (
    validate_user_activity_event,
    validate_aggregated_activity_event,
    generate_event_id,
)


class TestUserActivityEventValidation:

    def test_valid_event_accepted(self):
        event = {
            "userId": "user123",
            "activityType": "login",
            "timestamp": "2023-01-15T10:30:00Z",
        }
        validate_user_activity_event(event)

    def test_missing_user_id_rejected(self):
        with pytest.raises(ValidationError):
            validate_user_activity_event(
                {"activityType": "login", "timestamp": "2023-01-15T10:30:00Z"}
            )

    def test_missing_activity_type_rejected(self):
        with pytest.raises(ValidationError):
            validate_user_activity_event(
                {"userId": "user123", "timestamp": "2023-01-15T10:30:00Z"}
            )

    def test_missing_timestamp_rejected(self):
        with pytest.raises(ValidationError):
            validate_user_activity_event(
                {"userId": "user123", "activityType": "login"}
            )

    def test_empty_user_id_rejected(self):
        with pytest.raises(ValidationError):
            validate_user_activity_event(
                {"userId": "", "activityType": "login", "timestamp": "2023-01-15T10:30:00Z"}
            )

    def test_extra_properties_accepted(self):
        event = {
            "userId": "user123",
            "activityType": "login",
            "timestamp": "2023-01-15T10:30:00Z",
            "source": "mobile",
        }
        validate_user_activity_event(event)


class TestAggregatedActivityEventValidation:

    def test_valid_event_accepted(self):
        validate_aggregated_activity_event(
            {"userId": "user123", "date": "2023-01-15", "totalActivityCount": 5}
        )

    def test_invalid_date_format_rejected(self):
        with pytest.raises(ValidationError):
            validate_aggregated_activity_event(
                {"userId": "user123", "date": "01-15-2023", "totalActivityCount": 5}
            )

    def test_zero_count_rejected(self):
        with pytest.raises(ValidationError):
            validate_aggregated_activity_event(
                {"userId": "user123", "date": "2023-01-15", "totalActivityCount": 0}
            )

    def test_additional_properties_rejected(self):
        with pytest.raises(ValidationError):
            validate_aggregated_activity_event(
                {
                    "userId": "user123",
                    "date": "2023-01-15",
                    "totalActivityCount": 5,
                    "extra": "field",
                }
            )


class TestGenerateEventId:

    def test_deterministic_output(self):
        id_a = generate_event_id("user1", "login", "2023-01-15T10:00:00Z")
        id_b = generate_event_id("user1", "login", "2023-01-15T10:00:00Z")
        assert id_a == id_b

    def test_different_inputs_produce_different_ids(self):
        id_a = generate_event_id("user1", "login", "2023-01-15T10:00:00Z")
        id_b = generate_event_id("user2", "login", "2023-01-15T10:00:00Z")
        assert id_a != id_b

    def test_returns_64_char_hex(self):
        event_id = generate_event_id("u", "a", "2023-01-01T00:00:00Z")
        assert len(event_id) == 64
        int(event_id, 16)  # must not raise
