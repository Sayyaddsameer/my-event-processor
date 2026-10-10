"""End-to-end integration tests against LocalStack.

Pre-requisites:
  1. LocalStack must be running with SQS and DynamoDB services.
  2. The init-localstack.sh script must have created the queues and tables.
  3. Environment variables must point to the LocalStack endpoint.

Run with:  pytest tests/integration/ -v
"""

import json
import os
import time

import boto3
import pytest

from src.config import Config
from src.main import process_user_activity


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def aws_endpoint():
    return os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")


@pytest.fixture(scope="module")
def sqs_client(aws_endpoint):
    return boto3.client(
        "sqs",
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        endpoint_url=aws_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture(scope="module")
def dynamodb_client(aws_endpoint):
    return boto3.client(
        "dynamodb",
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        endpoint_url=aws_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture(scope="module")
def dynamodb_resource(aws_endpoint):
    return boto3.resource(
        "dynamodb",
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        endpoint_url=aws_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _key_attrs(table_name):
    if table_name == Config.DYNAMODB_TABLE_NAME:
        return ["userId", "date"]
    return ["eventId"]


@pytest.fixture(autouse=True)
def clean_tables(dynamodb_resource):
    """Wipe both DynamoDB tables before every test."""
    for name in [Config.DYNAMODB_TABLE_NAME, Config.PROCESSED_EVENTS_TABLE_NAME]:
        table = dynamodb_resource.Table(name)
        scan = table.scan()
        with table.batch_writer() as batch:
            for item in scan.get("Items", []):
                key = {k: item[k] for k in _key_attrs(name)}
                batch.delete_item(Key=key)
    yield


@pytest.fixture(autouse=True)
def purge_queues(sqs_client):
    """Purge all SQS queues before every test."""
    for url in [Config.INPUT_QUEUE_URL, Config.OUTPUT_QUEUE_URL, Config.DLQ_URL]:
        try:
            sqs_client.purge_queue(QueueUrl=url)
        except Exception:
            pass
    time.sleep(1)
    yield


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEndToEndActivityProcessing:

    def test_single_event_below_threshold(self, sqs_client, dynamodb_client):
        """One event increments the counter but does not trigger notification."""
        body = {
            "userId": "e2e-user-1",
            "activityType": "login",
            "timestamp": "2023-06-15T10:00:00Z",
        }
        result = process_user_activity(
            {"Records": [{"messageId": "int-1", "body": json.dumps(body)}]}, None
        )
        assert result["batchItemFailures"] == []

        item = dynamodb_client.get_item(
            TableName=Config.DYNAMODB_TABLE_NAME,
            Key={"userId": {"S": "e2e-user-1"}, "date": {"S": "2023-06-15"}},
        )
        assert item["Item"]["activityCount"]["N"] == "1"

        out = sqs_client.receive_message(
            QueueUrl=Config.OUTPUT_QUEUE_URL, MaxNumberOfMessages=1, WaitTimeSeconds=1
        )
        assert "Messages" not in out

    def test_threshold_crossing_publishes(self, sqs_client, dynamodb_client):
        """Crossing the threshold publishes an aggregated event."""
        threshold = Config.NOTIFICATION_THRESHOLD
        uid = "e2e-user-2"

        for i in range(threshold - 1):
            body = {
                "userId": uid,
                "activityType": f"act-{i}",
                "timestamp": f"2023-06-15T10:{i:02d}:00Z",
            }
            process_user_activity(
                {"Records": [{"messageId": f"int-{i}", "body": json.dumps(body)}]},
                None,
            )

        out = sqs_client.receive_message(
            QueueUrl=Config.OUTPUT_QUEUE_URL, MaxNumberOfMessages=1, WaitTimeSeconds=1
        )
        assert "Messages" not in out

        final = {
            "userId": uid,
            "activityType": "final",
            "timestamp": f"2023-06-15T10:{threshold - 1:02d}:00Z",
        }
        process_user_activity(
            {"Records": [{"messageId": "int-final", "body": json.dumps(final)}]},
            None,
        )

        item = dynamodb_client.get_item(
            TableName=Config.DYNAMODB_TABLE_NAME,
            Key={"userId": {"S": uid}, "date": {"S": "2023-06-15"}},
        )
        assert item["Item"]["activityCount"]["N"] == str(threshold)

        out = sqs_client.receive_message(
            QueueUrl=Config.OUTPUT_QUEUE_URL, MaxNumberOfMessages=1, WaitTimeSeconds=2
        )
        assert "Messages" in out
        msg = json.loads(out["Messages"][0]["Body"])
        assert msg["userId"] == uid
        assert msg["date"] == "2023-06-15"
        assert msg["totalActivityCount"] == threshold

    def test_malformed_event_lands_in_dlq(self, sqs_client):
        """Schema-invalid events are forwarded to the DLQ."""
        bad = json.dumps({"userId": "u1"})
        process_user_activity(
            {"Records": [{"messageId": "int-bad", "body": bad}]}, None
        )

        dlq = sqs_client.receive_message(
            QueueUrl=Config.DLQ_URL, MaxNumberOfMessages=1, WaitTimeSeconds=2
        )
        assert "Messages" in dlq

    def test_idempotency_no_double_count(self, sqs_client, dynamodb_client):
        """Reprocessing the exact same event must not increment the counter twice."""
        body = {
            "userId": "e2e-user-3",
            "activityType": "purchase",
            "timestamp": "2023-06-15T12:00:00Z",
        }
        payload = {"Records": [{"messageId": "int-idem", "body": json.dumps(body)}]}

        process_user_activity(payload, None)
        process_user_activity(payload, None)

        item = dynamodb_client.get_item(
            TableName=Config.DYNAMODB_TABLE_NAME,
            Key={"userId": {"S": "e2e-user-3"}, "date": {"S": "2023-06-15"}},
        )
        assert item["Item"]["activityCount"]["N"] == "1"
