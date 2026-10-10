"""Integration test configuration.

Sets environment variables that point the application at LocalStack
before any source modules are imported.
"""

import os


def pytest_configure(config):
    defaults = {
        "AWS_REGION": "us-east-1",
        "AWS_ENDPOINT_URL": "http://localhost:4566",
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "NOTIFICATION_THRESHOLD": "5",
        "DYNAMODB_TABLE_NAME": "UserActivityAggregates",
        "PROCESSED_EVENTS_TABLE_NAME": "ProcessedEvents",
        "INPUT_QUEUE_URL": "http://localhost:4566/000000000000/user-activity-input-queue",
        "OUTPUT_QUEUE_URL": "http://localhost:4566/000000000000/user-activity-output-queue",
        "DLQ_URL": "http://localhost:4566/000000000000/user-activity-dlq",
        "LOG_LEVEL": "DEBUG",
    }
    for key, value in defaults.items():
        os.environ.setdefault(key, value)
