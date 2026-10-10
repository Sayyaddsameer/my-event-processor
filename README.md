# User Activity Event Processor

A production-grade, event-driven backend service that consumes user activity events from an AWS SQS queue, maintains aggregated counters in DynamoDB, and publishes notification events when configurable thresholds are reached.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Project Structure](#project-structure)
4. [Prerequisites](#prerequisites)
5. [Local Development Setup](#local-development-setup)
6. [Running the Service](#running-the-service)
7. [Running Tests](#running-tests)
8. [Configuration Reference](#configuration-reference)
9. [API Reference](#api-reference)
10. [Event Schemas](#event-schemas)
11. [Error Handling Strategy](#error-handling-strategy)
12. [Idempotency](#idempotency)
13. [AWS Lambda Deployment](#aws-lambda-deployment)
14. [Design Decisions](#design-decisions)

---

## Overview

Modern distributed applications benefit from decoupled, asynchronous communication between services. This project implements a core building block of an event-driven architecture: an **activity aggregation service** that sits between raw event producers and downstream notification consumers.

**What the service does:**

1. Reads `UserActivityEvent` messages from an SQS input queue.
2. Validates each event against a JSON schema.
3. Routes malformed events to a Dead-Letter Queue (DLQ).
4. Atomically increments a per-user, per-day activity counter in DynamoDB.
5. When the counter meets or exceeds a configurable threshold, publishes an `AggregatedActivityEvent` to an SQS output queue.
6. Guarantees idempotent processing so that duplicate deliveries never corrupt state.

---

## Architecture

```
+-----------+      +-------------------------+      +-----------+
|  Producer | ---> | user-activity-input-queue| ---> | Lambda /  |
|  (events) |      |        (SQS)            |      | Processor |
+-----------+      +-------------------------+      +-----+-----+
                          |                               |
                   (malformed)                     +------+------+
                          |                        |             |
                          v                        v             v
                  +--------------+    +----------+----+   +------+------+
                  | user-activity|    | UserActivity   |   | user-activity|
                  |     -dlq     |    | Aggregates     |   | -output-queue|
                  |    (SQS)     |    | (DynamoDB)     |   |    (SQS)     |
                  +--------------+    +----------------+   +--------------+
                                                                  |
                                                                  v
                                                          +---------------+
                                                          |  Downstream   |
                                                          |  Notification |
                                                          |  Service      |
                                                          +---------------+
```

**Key components:**

| Component | Technology | Purpose |
|---|---|---|
| Input Queue | AWS SQS | Buffers raw user activity events |
| Dead-Letter Queue | AWS SQS | Captures permanently failed messages |
| Output Queue | AWS SQS | Carries aggregated notification events |
| Activity Table | DynamoDB | Stores per-user daily activity counters |
| Processed Events Table | DynamoDB | Tracks event IDs for idempotency |
| Processor | AWS Lambda / Container | Core business logic |

---

## Project Structure

```
my-event-processor/
|-- src/
|   |-- __init__.py
|   |-- config.py                  # Environment-based configuration
|   |-- main.py                    # Lambda handler and processing logic
|   |-- app.py                     # Flask health server + SQS poller (local)
|   |-- models/
|   |   |-- __init__.py
|   |   |-- event_schemas.py       # JSON schemas and validation helpers
|   |-- services/
|       |-- __init__.py
|       |-- dynamodb_service.py    # DynamoDB interaction layer
|       |-- sqs_service.py         # SQS interaction layer
|-- tests/
|   |-- unit/                      # Fast, isolated unit tests (moto)
|   |-- integration/               # End-to-end tests against LocalStack
|-- Dockerfile                     # Multi-stage container build
|-- docker-compose.yml             # Full local environment orchestration
|-- init-localstack.sh             # Auto-seeds SQS queues and DynamoDB tables
|-- requirements.txt               # Python dependencies
|-- pyproject.toml                 # Pytest and coverage configuration
|-- .env.example                   # Documented environment variables
|-- .gitignore
|-- README.md
|-- ARCHITECTURE.md                # In-depth architectural decisions
```

---

## Prerequisites

- **Docker** and **Docker Compose** (v2 or later)
- **Python 3.11+** (for running tests outside Docker)
- **pip** (for installing dependencies locally)

---

## Local Development Setup

### 1. Clone the repository

```bash
git clone <repository-url>
cd my-event-processor
```

### 2. Create environment file

```bash
cp .env.example .env
```

The defaults in `.env.example` are ready to use with LocalStack. Adjust values only if you have changed ports or resource names.

### 3. Start the full stack

```bash
docker-compose up --build
```

This single command:

- Starts **LocalStack** with SQS and DynamoDB services.
- Waits for LocalStack to become healthy.
- Runs **init-localstack.sh** to create queues (with DLQ redrive policy) and DynamoDB tables.
- Builds and starts the **processor-service** container, which polls the input queue and exposes the health endpoint.

### 4. Verify the stack

```bash
# Health check
curl http://localhost:8000/health
# Expected: {"status": "OK", "service": "UserActivityProcessor"}

# List SQS queues in LocalStack
aws --endpoint-url=http://localhost:4566 sqs list-queues --region us-east-1

# Describe DynamoDB tables
aws --endpoint-url=http://localhost:4566 dynamodb list-tables --region us-east-1
```

---

## Running the Service

### Containerized (recommended for local development)

```bash
docker-compose up --build
```

### Sending a test event

```bash
aws --endpoint-url=http://localhost:4566 sqs send-message \
  --queue-url http://localhost:4566/000000000000/user-activity-input-queue \
  --message-body '{"userId":"user42","activityType":"login","timestamp":"2023-06-15T10:00:00Z"}' \
  --region us-east-1
```

Watch the processor-service logs to see the event being consumed and the DynamoDB counter incremented.

---

## Running Tests

### Install dependencies locally

```bash
pip install -r requirements.txt
```

### Unit tests (no external services required)

```bash
pytest tests/unit/ -v --cov=src --cov-report=term-missing
```

Unit tests use **moto** to mock AWS services in-process.

### Integration tests (requires LocalStack)

Make sure LocalStack is running first:

```bash
docker-compose up localstack -d
# Wait for health check to pass
```

Then run:

```bash
pytest tests/integration/ -v
```

Integration tests call the Lambda handler directly against real LocalStack queues and tables.

### Full test suite with coverage

```bash
pytest --cov=src --cov-report=term-missing --cov-report=html
```

Open `htmlcov/index.html` to browse the coverage report.

---

## Configuration Reference

| Variable | Default | Description |
|---|---|---|
| `AWS_REGION` | `us-east-1` | AWS region for all SDK calls |
| `AWS_ENDPOINT_URL` | _(none)_ | Custom endpoint for LocalStack |
| `AWS_ACCESS_KEY_ID` | _(none)_ | AWS credential (use `test` for LocalStack) |
| `AWS_SECRET_ACCESS_KEY` | _(none)_ | AWS credential (use `test` for LocalStack) |
| `NOTIFICATION_THRESHOLD` | `5` | Daily activity count that triggers a notification |
| `DYNAMODB_TABLE_NAME` | `UserActivityAggregates` | DynamoDB table for counters |
| `PROCESSED_EVENTS_TABLE_NAME` | `ProcessedEvents` | DynamoDB table for idempotency |
| `INPUT_QUEUE_URL` | _(empty)_ | SQS URL of the input queue |
| `OUTPUT_QUEUE_URL` | _(empty)_ | SQS URL of the output queue |
| `DLQ_URL` | _(empty)_ | SQS URL of the dead-letter queue |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `POLL_INTERVAL_SECONDS` | `2` | SQS long-poll wait time (local mode) |
| `HEALTH_CHECK_PORT` | `8000` | Port for the Flask health server |

All configuration is loaded from environment variables. Nothing is hardcoded.

---

## API Reference

### Health Check

```
GET /health
```

**Response (200 OK):**

```json
{
  "status": "OK",
  "service": "UserActivityProcessor"
}
```

---

## Event Schemas

### Input: UserActivityEvent

```json
{
  "userId": "user42",
  "activityType": "login",
  "timestamp": "2023-06-15T10:00:00Z"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `userId` | string | Yes | Unique user identifier |
| `activityType` | string | Yes | Type of activity (login, purchase, etc.) |
| `timestamp` | string (ISO 8601) | Yes | When the activity occurred |

### Output: AggregatedActivityEvent

```json
{
  "userId": "user42",
  "date": "2023-06-15",
  "totalActivityCount": 5
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `userId` | string | Yes | Unique user identifier |
| `date` | string (YYYY-MM-DD) | Yes | Date of aggregated activity |
| `totalActivityCount` | integer (>= 1) | Yes | Cumulative daily activity count |

---

## Error Handling Strategy

The service distinguishes between two categories of failures:

**Permanent failures** (routed to DLQ immediately):
- Malformed JSON that cannot be parsed
- Events that fail schema validation
- Missing required fields

**Transient failures** (retried via Lambda / SQS):
- DynamoDB write timeouts or throttling
- SQS publish failures
- Network connectivity issues

The Lambda handler returns a **partial batch failure** response, meaning only the messages that encountered transient errors are retried. Successfully processed messages and permanently failed messages (already routed to DLQ) are removed from the queue.

The input queue is configured with a **redrive policy** that moves messages to the DLQ after 3 failed receive attempts, providing a safety net for messages that repeatedly fail.

---

## Idempotency

SQS provides **at-least-once delivery**, meaning the same message may be delivered more than once. Without idempotency, this would cause activity counters to be incremented multiple times for a single logical event.

The service solves this with a **ProcessedEvents** DynamoDB table:

1. A deterministic event ID is computed from `userId + activityType + timestamp`.
2. Before processing, the service attempts a **conditional put** (`attribute_not_exists`) to the ProcessedEvents table.
3. If the put succeeds, the event is new and processing continues.
4. If the put fails with `ConditionalCheckFailedException`, the event has already been processed and is silently skipped.
5. Entries in ProcessedEvents have a **TTL** of 24 hours, so the table self-cleans.

This approach guarantees exactly-once semantics for business logic, even though the underlying transport provides at-least-once delivery.

---

## AWS Lambda Deployment

### As a container image

1. Build the Docker image:

```bash
docker build -t user-activity-processor .
```

2. Push to Amazon ECR.
3. Create a Lambda function using the container image.
4. Configure the SQS trigger with:
   - **Batch size:** 10
   - **Report batch item failures:** Enabled
   - **Maximum batching window:** 5 seconds
5. Set environment variables (see Configuration Reference).
6. Remove `AWS_ENDPOINT_URL` (not needed in production).

### As a zip deployment

1. Package `src/` with dependencies into a zip.
2. Set the handler to `src.main.process_user_activity`.
3. Configure environment variables and SQS trigger as above.

### IAM permissions required

```json
{
  "Effect": "Allow",
  "Action": [
    "sqs:ReceiveMessage",
    "sqs:DeleteMessage",
    "sqs:GetQueueAttributes",
    "sqs:SendMessage"
  ],
  "Resource": [
    "arn:aws:sqs:*:*:user-activity-input-queue",
    "arn:aws:sqs:*:*:user-activity-output-queue",
    "arn:aws:sqs:*:*:user-activity-dlq"
  ]
}
```

```json
{
  "Effect": "Allow",
  "Action": [
    "dynamodb:UpdateItem",
    "dynamodb:GetItem",
    "dynamodb:PutItem"
  ],
  "Resource": [
    "arn:aws:dynamodb:*:*:table/UserActivityAggregates",
    "arn:aws:dynamodb:*:*:table/ProcessedEvents"
  ]
}
```

---

## Design Decisions

| Decision | Rationale |
|---|---|
| DynamoDB `ADD` for counters | Single atomic operation, no read-modify-write race conditions |
| Separate idempotency table | Keeps the aggregation table clean; TTL enables automatic cleanup |
| Content-based event ID | More robust than SQS message ID for deduplication across retries |
| Partial batch failure response | Lets Lambda retry only the failed messages, not the entire batch |
| Structured JSON logging | Machine-parseable logs for CloudWatch Insights and log aggregation |
| Flask for local health check | Lightweight, well-known, and easy to test |
| Multi-stage Docker build | Reduces final image size by excluding build tools |

For deeper architectural analysis, see [ARCHITECTURE.md](./ARCHITECTURE.md).
