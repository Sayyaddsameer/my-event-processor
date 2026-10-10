"""Unit tests for the DynamoDB service layer.

Uses *moto* to provide an in-memory DynamoDB backend so that no real AWS
resources or LocalStack instance are required.
"""

import pytest
import boto3
from moto import mock_aws
from unittest.mock import patch

from src.config import Config
from src.services.dynamodb_service import (
    increment_activity_count,
    get_activity_count,
    mark_event_processed,
)


@pytest.fixture
def dynamodb_tables():
    with mock_aws():
        with patch.object(Config, "AWS_ENDPOINT_URL", None), \
             patch.object(Config, "AWS_REGION", "us-east-1"):

            dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

            dynamodb.create_table(
                TableName=Config.DYNAMODB_TABLE_NAME,
                KeySchema=[
                    {"AttributeName": "userId", "KeyType": "HASH"},
                    {"AttributeName": "date", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "userId", "AttributeType": "S"},
                    {"AttributeName": "date", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )

            dynamodb.create_table(
                TableName=Config.PROCESSED_EVENTS_TABLE_NAME,
                KeySchema=[{"AttributeName": "eventId", "KeyType": "HASH"}],
                AttributeDefinitions=[
                    {"AttributeName": "eventId", "AttributeType": "S"}
                ],
                BillingMode="PAY_PER_REQUEST",
            )

            yield dynamodb


class TestIncrementActivityCount:

    def test_first_increment_returns_one(self, dynamodb_tables):
        with patch.object(Config, "AWS_ENDPOINT_URL", None):
            assert increment_activity_count("user1", "2023-01-15") == 1

    def test_successive_increments(self, dynamodb_tables):
        with patch.object(Config, "AWS_ENDPOINT_URL", None):
            increment_activity_count("user1", "2023-01-15")
            increment_activity_count("user1", "2023-01-15")
            assert increment_activity_count("user1", "2023-01-15") == 3

    def test_different_users_independent(self, dynamodb_tables):
        with patch.object(Config, "AWS_ENDPOINT_URL", None):
            assert increment_activity_count("user1", "2023-01-15") == 1
            assert increment_activity_count("user2", "2023-01-15") == 1

    def test_different_dates_independent(self, dynamodb_tables):
        with patch.object(Config, "AWS_ENDPOINT_URL", None):
            increment_activity_count("user1", "2023-01-15")
            assert increment_activity_count("user1", "2023-01-16") == 1


class TestGetActivityCount:

    def test_returns_zero_when_missing(self, dynamodb_tables):
        with patch.object(Config, "AWS_ENDPOINT_URL", None):
            assert get_activity_count("user1", "2023-01-15") == 0

    def test_returns_correct_count(self, dynamodb_tables):
        with patch.object(Config, "AWS_ENDPOINT_URL", None):
            increment_activity_count("user1", "2023-01-15")
            increment_activity_count("user1", "2023-01-15")
            assert get_activity_count("user1", "2023-01-15") == 2


class TestMarkEventProcessed:

    def test_first_call_returns_true(self, dynamodb_tables):
        with patch.object(Config, "AWS_ENDPOINT_URL", None):
            assert mark_event_processed("evt-abc-123") is True

    def test_second_call_returns_false(self, dynamodb_tables):
        with patch.object(Config, "AWS_ENDPOINT_URL", None):
            mark_event_processed("evt-abc-123")
            assert mark_event_processed("evt-abc-123") is False
