"""DynamoDB interaction layer for activity aggregation and idempotency."""

import logging
import time

import boto3
from botocore.exceptions import ClientError

from src.config import Config

logger = logging.getLogger(__name__)


def _get_dynamodb_resource():
    """Build a DynamoDB resource, optionally targeting a custom endpoint."""
    kwargs = {"region_name": Config.AWS_REGION}
    if Config.AWS_ENDPOINT_URL:
        kwargs["endpoint_url"] = Config.AWS_ENDPOINT_URL
    return boto3.resource("dynamodb", **kwargs)


def increment_activity_count(user_id: str, activity_date: str) -> int:
    """Atomically increment the daily activity counter and return the new value.

    Uses DynamoDB's ``ADD`` expression which is a single, atomic operation,
    guaranteeing correctness even under concurrent writes.
    """
    dynamodb = _get_dynamodb_resource()
    table = dynamodb.Table(Config.DYNAMODB_TABLE_NAME)
    try:
        response = table.update_item(
            Key={"userId": user_id, "date": activity_date},
            UpdateExpression="ADD activityCount :val",
            ExpressionAttributeValues={":val": 1},
            ReturnValues="UPDATED_NEW",
        )
        new_count = int(response["Attributes"]["activityCount"])
        logger.info(
            "Activity count incremented",
            extra={"userId": user_id, "date": activity_date, "newCount": new_count},
        )
        return new_count
    except ClientError as exc:
        logger.error(
            "DynamoDB update failed",
            extra={"userId": user_id, "date": activity_date, "error": str(exc)},
        )
        raise


def get_activity_count(user_id: str, activity_date: str) -> int:
    """Read the current daily activity count. Returns 0 when no record exists."""
    dynamodb = _get_dynamodb_resource()
    table = dynamodb.Table(Config.DYNAMODB_TABLE_NAME)
    response = table.get_item(Key={"userId": user_id, "date": activity_date})
    item = response.get("Item")
    if item is None:
        return 0
    return int(item.get("activityCount", 0))


def mark_event_processed(event_id: str, ttl_seconds: int = 86400) -> bool:
    """Attempt to record an event id using a conditional write.

    Returns ``True`` when the event has not been seen before (first time).
    Returns ``False`` when the event was already recorded (duplicate).

    A TTL attribute is set so that stale entries are automatically purged by
    DynamoDB's time-to-live mechanism.
    """
    dynamodb = _get_dynamodb_resource()
    table = dynamodb.Table(Config.PROCESSED_EVENTS_TABLE_NAME)
    expiry = int(time.time()) + ttl_seconds
    try:
        table.put_item(
            Item={"eventId": event_id, "ttl": expiry},
            ConditionExpression="attribute_not_exists(eventId)",
        )
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info("Duplicate event detected, skipping", extra={"eventId": event_id})
            return False
        raise
