import os


class Config:
    """Centralized configuration loaded exclusively from environment variables.

    Class attributes are resolved once at import time. For testing, patch
    individual attributes with ``unittest.mock.patch.object``.
    """

    AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
    AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL")

    NOTIFICATION_THRESHOLD = int(os.environ.get("NOTIFICATION_THRESHOLD", "5"))

    DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME", "UserActivityAggregates")
    PROCESSED_EVENTS_TABLE_NAME = os.environ.get(
        "PROCESSED_EVENTS_TABLE_NAME", "ProcessedEvents"
    )

    INPUT_QUEUE_URL = os.environ.get("INPUT_QUEUE_URL", "")
    OUTPUT_QUEUE_URL = os.environ.get("OUTPUT_QUEUE_URL", "")
    DLQ_URL = os.environ.get("DLQ_URL", "")

    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
    POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "2"))
    HEALTH_CHECK_PORT = int(os.environ.get("HEALTH_CHECK_PORT", "8000"))
