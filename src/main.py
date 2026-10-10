"""Lambda handler and core event processing logic.

This module is the primary entry point for the AWS Lambda function that
processes user activity events arriving via SQS. It also exposes a Lambda-
compatible health check handler for operational probing.
"""

import json
import logging
import os
import uuid
from datetime import datetime

from jsonschema import ValidationError
from pythonjsonlogger import jsonlogger

from src.config import Config
from src.models.event_schemas import validate_user_activity_event, generate_event_id
from src.services.dynamodb_service import increment_activity_count, mark_event_processed
from src.services.sqs_service import send_message

# ---------------------------------------------------------------------------
# Structured JSON logging
# ---------------------------------------------------------------------------
_root = logging.getLogger()
if not any(isinstance(h.formatter, jsonlogger.JsonFormatter) for h in _root.handlers):
    _handler = logging.StreamHandler()
    _formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        rename_fields={"asctime": "timestamp", "levelname": "level", "name": "logger"},
    )
    _handler.setFormatter(_formatter)
    _root.handlers = [_handler]
_root.setLevel(Config.LOG_LEVEL)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lambda handlers
# ---------------------------------------------------------------------------


def process_user_activity(event, context):
    """AWS Lambda handler for SQS-triggered user activity processing.

    For each record in the batch the handler:
    1. Parses and validates the JSON payload.
    2. Checks idempotency so duplicate deliveries are harmless.
    3. Atomically increments the daily activity counter in DynamoDB.
    4. Publishes an aggregated event when the counter meets or exceeds the
       configurable notification threshold.

    Malformed or invalid messages are routed to the Dead-Letter Queue.
    Transient errors (DynamoDB timeouts, SQS publish failures) are surfaced
    via the partial batch failure response so that Lambda retries only the
    affected messages.
    """
    correlation_id = str(uuid.uuid4())
    records = event.get("Records", [])
    logger.info(
        "Processing batch started",
        extra={"correlationId": correlation_id, "recordCount": len(records)},
    )

    batch_item_failures = []

    for record in records:
        message_id = record.get("messageId", "unknown")
        message_body = record.get("body", "")

        try:
            _process_single_record(message_body, message_id, correlation_id)

        except (ValidationError, json.JSONDecodeError, KeyError, ValueError) as exc:
            logger.error(
                "Permanent processing failure, routing to DLQ",
                extra={
                    "correlationId": correlation_id,
                    "messageId": message_id,
                    "error": str(exc),
                    "rawBody": message_body,
                },
            )
            _route_to_dlq(message_body, message_id, correlation_id)

        except Exception as exc:
            logger.error(
                "Transient error, marking for retry",
                extra={
                    "correlationId": correlation_id,
                    "messageId": message_id,
                    "error": str(exc),
                },
            )
            batch_item_failures.append({"itemIdentifier": message_id})

    logger.info(
        "Processing batch completed",
        extra={
            "correlationId": correlation_id,
            "failureCount": len(batch_item_failures),
        },
    )
    return {"batchItemFailures": batch_item_failures}


def health_check_handler(event, context):
    """Lambda-compatible health probe."""
    return {
        "statusCode": 200,
        "body": json.dumps({"status": "OK", "service": "UserActivityProcessor"}),
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _process_single_record(message_body: str, message_id: str, correlation_id: str):
    """Parse, validate, deduplicate, aggregate, and conditionally publish."""
    user_activity_event = json.loads(message_body)
    validate_user_activity_event(user_activity_event)

    user_id = user_activity_event["userId"]
    activity_type = user_activity_event["activityType"]
    timestamp_str = user_activity_event["timestamp"]
    activity_date = datetime.fromisoformat(
        timestamp_str.replace("Z", "+00:00")
    ).strftime("%Y-%m-%d")

    event_id = generate_event_id(user_id, activity_type, timestamp_str)

    logger.info(
        "Processing event",
        extra={
            "correlationId": correlation_id,
            "messageId": message_id,
            "eventId": event_id,
            "userId": user_id,
            "activityType": activity_type,
            "date": activity_date,
        },
    )

    # Idempotency guard
    is_new = mark_event_processed(event_id)
    if not is_new:
        logger.info(
            "Duplicate event skipped",
            extra={
                "correlationId": correlation_id,
                "eventId": event_id,
                "userId": user_id,
            },
        )
        return

    new_count = increment_activity_count(user_id, activity_date)

    if new_count >= Config.NOTIFICATION_THRESHOLD:
        aggregated_event = {
            "userId": user_id,
            "date": activity_date,
            "totalActivityCount": new_count,
        }
        send_message(Config.OUTPUT_QUEUE_URL, json.dumps(aggregated_event))
        logger.info(
            "Aggregated event published",
            extra={
                "correlationId": correlation_id,
                "userId": user_id,
                "date": activity_date,
                "totalActivityCount": new_count,
            },
        )


def _route_to_dlq(message_body: str, message_id: str, correlation_id: str):
    """Forward a permanently failed message to the Dead-Letter Queue."""
    try:
        send_message(Config.DLQ_URL, message_body)
        logger.info(
            "Message routed to DLQ",
            extra={"correlationId": correlation_id, "messageId": message_id},
        )
    except Exception as dlq_exc:
        logger.error(
            "Failed to route message to DLQ",
            extra={
                "correlationId": correlation_id,
                "messageId": message_id,
                "error": str(dlq_exc),
            },
        )
