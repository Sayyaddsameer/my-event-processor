"""SQS interaction layer for sending, receiving, and deleting messages."""

import logging

import boto3
from botocore.exceptions import ClientError

from src.config import Config

logger = logging.getLogger(__name__)


def _get_sqs_client():
    """Build an SQS client, optionally targeting a custom endpoint."""
    kwargs = {"region_name": Config.AWS_REGION}
    if Config.AWS_ENDPOINT_URL:
        kwargs["endpoint_url"] = Config.AWS_ENDPOINT_URL
    return boto3.client("sqs", **kwargs)


def send_message(
    queue_url: str, message_body: str, message_attributes: dict = None
) -> dict:
    """Publish a single message to the specified SQS queue."""
    client = _get_sqs_client()
    params = {"QueueUrl": queue_url, "MessageBody": message_body}
    if message_attributes:
        params["MessageAttributes"] = message_attributes
    try:
        response = client.send_message(**params)
        logger.info(
            "Message sent to SQS",
            extra={"queueUrl": queue_url, "messageId": response.get("MessageId")},
        )
        return response
    except ClientError as exc:
        logger.error(
            "Failed to send SQS message",
            extra={"queueUrl": queue_url, "error": str(exc)},
        )
        raise


def receive_messages(
    queue_url: str, max_messages: int = 10, wait_time: int = 20
) -> list:
    """Long-poll the queue and return up to *max_messages* messages."""
    client = _get_sqs_client()
    try:
        response = client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
        )
        return response.get("Messages", [])
    except ClientError as exc:
        logger.error(
            "Failed to receive SQS messages",
            extra={"queueUrl": queue_url, "error": str(exc)},
        )
        raise


def delete_message(queue_url: str, receipt_handle: str) -> None:
    """Delete a message from the queue after successful processing."""
    client = _get_sqs_client()
    try:
        client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    except ClientError as exc:
        logger.error(
            "Failed to delete SQS message",
            extra={"queueUrl": queue_url, "error": str(exc)},
        )
        raise
