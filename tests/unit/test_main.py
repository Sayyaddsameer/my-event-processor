"""Unit tests for the Lambda handler and core processing logic."""

import json
import pytest
from unittest.mock import patch, MagicMock

from src.main import process_user_activity, health_check_handler
from src.config import Config


def _build_sqs_event(*bodies):
    """Build a minimal SQS-style Lambda event from one or more payloads."""
    records = []
    for idx, body in enumerate(bodies):
        records.append(
            {
                "messageId": f"msg-{idx}",
                "body": body if isinstance(body, str) else json.dumps(body),
            }
        )
    return {"Records": records}


def _valid_payload(
    user_id="user123", activity_type="login", timestamp="2023-01-15T10:30:00Z"
):
    return {"userId": user_id, "activityType": activity_type, "timestamp": timestamp}


class TestProcessUserActivity:

    @patch("src.main.send_message")
    @patch("src.main.increment_activity_count")
    @patch("src.main.mark_event_processed", return_value=True)
    def test_below_threshold_no_publish(self, mock_mark, mock_incr, mock_send):
        mock_incr.return_value = 3
        event = _build_sqs_event(_valid_payload())

        with patch.object(Config, "NOTIFICATION_THRESHOLD", 5):
            result = process_user_activity(event, None)

        mock_incr.assert_called_once_with("user123", "2023-01-15")
        mock_send.assert_not_called()
        assert result["batchItemFailures"] == []

    @patch("src.main.send_message")
    @patch("src.main.increment_activity_count")
    @patch("src.main.mark_event_processed", return_value=True)
    def test_at_threshold_publishes(self, mock_mark, mock_incr, mock_send):
        mock_incr.return_value = 5
        event = _build_sqs_event(_valid_payload())

        with patch.object(Config, "NOTIFICATION_THRESHOLD", 5), \
             patch.object(Config, "OUTPUT_QUEUE_URL", "http://out"):
            result = process_user_activity(event, None)

        mock_send.assert_called_once()
        body = json.loads(mock_send.call_args[0][1])
        assert body["userId"] == "user123"
        assert body["totalActivityCount"] == 5
        assert result["batchItemFailures"] == []

    @patch("src.main.send_message")
    @patch("src.main.increment_activity_count")
    @patch("src.main.mark_event_processed", return_value=True)
    def test_above_threshold_publishes(self, mock_mark, mock_incr, mock_send):
        mock_incr.return_value = 8
        event = _build_sqs_event(_valid_payload())

        with patch.object(Config, "NOTIFICATION_THRESHOLD", 5), \
             patch.object(Config, "OUTPUT_QUEUE_URL", "http://out"):
            result = process_user_activity(event, None)

        body = json.loads(mock_send.call_args[0][1])
        assert body["totalActivityCount"] == 8

    @patch("src.main.send_message")
    @patch("src.main.mark_event_processed")
    @patch("src.main.increment_activity_count")
    def test_malformed_json_goes_to_dlq(self, mock_incr, mock_mark, mock_send):
        event = _build_sqs_event("not valid json {{{")

        with patch.object(Config, "DLQ_URL", "http://dlq"):
            result = process_user_activity(event, None)

        mock_incr.assert_not_called()
        mock_send.assert_called_once_with("http://dlq", "not valid json {{{")
        assert result["batchItemFailures"] == []

    @patch("src.main.send_message")
    @patch("src.main.mark_event_processed")
    @patch("src.main.increment_activity_count")
    def test_schema_failure_goes_to_dlq(self, mock_incr, mock_mark, mock_send):
        bad_event = {"userId": "user123"}
        event = _build_sqs_event(bad_event)

        with patch.object(Config, "DLQ_URL", "http://dlq"):
            result = process_user_activity(event, None)

        mock_incr.assert_not_called()
        mock_send.assert_called_once()
        assert result["batchItemFailures"] == []

    @patch("src.main.send_message")
    @patch("src.main.increment_activity_count")
    @patch("src.main.mark_event_processed", return_value=False)
    def test_duplicate_event_skipped(self, mock_mark, mock_incr, mock_send):
        event = _build_sqs_event(_valid_payload())

        result = process_user_activity(event, None)

        mock_mark.assert_called_once()
        mock_incr.assert_not_called()
        mock_send.assert_not_called()
        assert result["batchItemFailures"] == []

    @patch("src.main.send_message")
    @patch("src.main.increment_activity_count")
    @patch("src.main.mark_event_processed", return_value=True)
    def test_transient_error_batch_failure(self, mock_mark, mock_incr, mock_send):
        mock_incr.side_effect = Exception("DynamoDB timeout")
        event = _build_sqs_event(_valid_payload())

        result = process_user_activity(event, None)

        assert len(result["batchItemFailures"]) == 1
        assert result["batchItemFailures"][0]["itemIdentifier"] == "msg-0"

    @patch("src.main.send_message")
    @patch("src.main.increment_activity_count")
    @patch("src.main.mark_event_processed", return_value=True)
    def test_multiple_records(self, mock_mark, mock_incr, mock_send):
        mock_incr.return_value = 2
        event = _build_sqs_event(
            _valid_payload("u1", "login", "2023-01-15T10:00:00Z"),
            _valid_payload("u2", "purchase", "2023-01-15T11:00:00Z"),
        )

        with patch.object(Config, "NOTIFICATION_THRESHOLD", 5):
            result = process_user_activity(event, None)

        assert mock_incr.call_count == 2
        assert result["batchItemFailures"] == []

    @patch("src.main.send_message")
    @patch("src.main.increment_activity_count")
    @patch("src.main.mark_event_processed", return_value=True)
    def test_mixed_success_and_failure(self, mock_mark, mock_incr, mock_send):
        """One valid record followed by one that triggers a transient error."""
        mock_incr.side_effect = [2, Exception("timeout")]
        event = _build_sqs_event(
            _valid_payload("u1", "a", "2023-01-15T10:00:00Z"),
            _valid_payload("u2", "b", "2023-01-15T11:00:00Z"),
        )

        with patch.object(Config, "NOTIFICATION_THRESHOLD", 5):
            result = process_user_activity(event, None)

        assert len(result["batchItemFailures"]) == 1
        assert result["batchItemFailures"][0]["itemIdentifier"] == "msg-1"


class TestHealthCheckHandler:

    def test_returns_200(self):
        resp = health_check_handler({}, None)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["status"] == "OK"
        assert body["service"] == "UserActivityProcessor"
