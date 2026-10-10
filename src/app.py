"""Local development entry point.

Runs a Flask server for the ``GET /health`` endpoint alongside an SQS
polling loop that simulates Lambda invocations by reading messages from
the input queue and delegating to the core processing handler.
"""

import json
import logging
import signal
import sys
import threading
import time

from flask import Flask, jsonify

from src.config import Config
from src.main import process_user_activity
from src.services.sqs_service import receive_messages, delete_message

logger = logging.getLogger(__name__)

app = Flask(__name__)

_shutdown_event = threading.Event()


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint returning 200 OK when the service is alive."""
    return jsonify({"status": "OK", "service": "UserActivityProcessor"}), 200


def _poll_sqs():
    """Continuously poll the input SQS queue and invoke the processing handler."""
    logger.info("SQS polling started", extra={"queueUrl": Config.INPUT_QUEUE_URL})
    while not _shutdown_event.is_set():
        try:
            messages = receive_messages(
                Config.INPUT_QUEUE_URL,
                max_messages=10,
                wait_time=Config.POLL_INTERVAL_SECONDS,
            )
            if not messages:
                continue

            # Build a synthetic Lambda event from the raw SQS messages
            records = [
                {
                    "messageId": msg["MessageId"],
                    "body": msg["Body"],
                    "receiptHandle": msg["ReceiptHandle"],
                }
                for msg in messages
            ]

            result = process_user_activity({"Records": records}, None)

            # Delete successfully processed messages from the queue
            failed_ids = {
                item["itemIdentifier"]
                for item in result.get("batchItemFailures", [])
            }
            for msg in messages:
                if msg["MessageId"] not in failed_ids:
                    delete_message(Config.INPUT_QUEUE_URL, msg["ReceiptHandle"])

        except Exception as exc:
            logger.error("Error in SQS polling loop", extra={"error": str(exc)})
            time.sleep(Config.POLL_INTERVAL_SECONDS)


def _handle_signal(signum, frame):
    """Gracefully shut down on SIGTERM / SIGINT."""
    logger.info("Shutdown signal received")
    _shutdown_event.set()
    sys.exit(0)


def main():
    """Start the SQS poller thread and the Flask health-check server."""
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    poller_thread = threading.Thread(target=_poll_sqs, daemon=True)
    poller_thread.start()

    logger.info(
        "Starting health check server",
        extra={"port": Config.HEALTH_CHECK_PORT},
    )
    app.run(host="0.0.0.0", port=Config.HEALTH_CHECK_PORT, use_reloader=False)


if __name__ == "__main__":
    main()
