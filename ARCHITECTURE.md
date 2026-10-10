# Architecture Decision Record

This document captures the key architectural decisions, trade-offs, and design rationale for the User Activity Event Processor.

---

## 1. Event-Driven Architecture Rationale

### Why EDA?

Traditional request-response architectures create tight coupling between producers and consumers. In our scenario (user activity tracking), the volume and variety of events can vary dramatically. An event-driven approach provides:

- **Temporal decoupling**: producers and consumers do not need to be available simultaneously.
- **Load leveling**: SQS absorbs traffic spikes, allowing the processor to consume at its own pace.
- **Independent scaling**: the processor can scale horizontally without affecting upstream services.
- **Failure isolation**: a failure in the processor does not cascade to the event producer.

### Alternative considered: Direct API calls

A synchronous REST API would introduce latency for producers and require them to handle processor downtime. The asynchronous SQS-based approach was chosen for its resilience and throughput characteristics.

---

## 2. Message Processing Strategy

### Partial batch failure reporting

Rather than failing the entire batch when a single message encounters a transient error, the handler returns a `batchItemFailures` response. This is a Lambda-native feature that allows SQS to selectively retry only the failed messages while acknowledging the successful ones.

**Trade-off**: This requires distinguishing between permanent and transient errors at the application level. The classification logic is intentionally conservative -- only well-known parse/validation errors are treated as permanent; everything else is assumed transient.

### Message visibility and retry behavior

The input queue is configured with a redrive policy (maxReceiveCount = 3). Combined with Lambda's built-in retry on invocation failure, a message gets up to 3 delivery attempts before being moved to the DLQ by SQS itself.

Application-level DLQ routing (for malformed messages) happens immediately on the first attempt, bypassing the retry cycle entirely.

---

## 3. DynamoDB Data Model

### UserActivityAggregates table

| Attribute | Type | Key | Description |
|---|---|---|---|
| userId | String | Partition Key | User identifier |
| date | String | Sort Key | Activity date (YYYY-MM-DD) |
| activityCount | Number | -- | Cumulative daily count |

**Partition key choice**: `userId` distributes writes across partitions naturally, assuming a large user base. The sort key `date` enables efficient queries for a user's activity history.

**Billing mode**: PAY_PER_REQUEST was chosen to eliminate capacity planning for a development/demo workload. For sustained production traffic, PROVISIONED with auto-scaling would reduce cost.

### ProcessedEvents table

| Attribute | Type | Key | Description |
|---|---|---|---|
| eventId | String | Partition Key | SHA-256 hash of event content |
| ttl | Number | -- | Unix timestamp for automatic expiry |

The TTL attribute enables DynamoDB's built-in time-to-live mechanism to automatically delete entries after 24 hours, keeping the table from growing unboundedly.

### Why not a single table?

Separating aggregation data from idempotency records provides:

- **Independent lifecycle management**: ProcessedEvents entries are ephemeral (24h TTL), while aggregation data is long-lived.
- **Schema clarity**: Each table has a single, well-defined purpose.
- **Operational isolation**: Issues with the idempotency table (accidental deletion, throttling) do not affect aggregation queries.

---

## 4. Idempotency Mechanism

### Approach: Conditional writes with content-based hashing

The event ID is a SHA-256 hash of `{userId, activityType, timestamp}`. This means:

- The same logical event always produces the same ID, regardless of how many times it is delivered.
- Different events from the same user at different times produce different IDs.
- The hash is collision-resistant for practical purposes.

### Why not use SQS MessageId?

SQS generates a new MessageId for each `SendMessage` call. If a producer retries sending the same logical event (for example, due to a network timeout after the initial send), the two copies will have different MessageIds. Content-based hashing catches these producer-side duplicates as well.

### Why not use SQS FIFO queues?

FIFO queues provide exactly-once processing semantics natively, but they have lower throughput limits (300 messages/second without batching, 3,000 with). The standard queue + application-level idempotency approach was chosen for higher throughput flexibility.

**Trade-off**: Application-level idempotency adds a DynamoDB write per event. At scale, this cost must be weighed against the throughput ceiling of FIFO queues.

---

## 5. Atomicity of Counter Increments

DynamoDB's `ADD` expression in `UpdateItem` is a single atomic operation. It does not require reading the current value first, so there is no read-modify-write race condition. Two concurrent Lambda invocations updating the same `(userId, date)` partition will both succeed and the final count will be correct.

This is a critical property for a service that may scale to many concurrent invocations.

---

## 6. Notification Threshold Semantics

The service publishes an aggregated event every time the counter is at or above the threshold after an increment. This means:

- The 5th event triggers a notification.
- The 6th, 7th, etc. also trigger notifications.

### Why not notify only on first crossing?

Tracking whether a notification has already been sent for a given (userId, date) would require either:

- An additional `notified` flag in DynamoDB with a conditional update, adding complexity.
- A comparison between old and new count values, which is not available from the atomic `ADD` operation without a separate read.

The current approach keeps the processing logic simple and stateless. Downstream consumers are expected to handle repeated notifications idempotently (for example, by deduplicating on `userId + date`).

---

## 7. Local Development Architecture

The Docker Compose environment mirrors the production topology as closely as possible:

- **LocalStack** provides API-compatible SQS and DynamoDB endpoints.
- **init-localstack.sh** runs automatically when LocalStack is healthy, creating all required queues and tables with the correct configuration (including the DLQ redrive policy).
- The **processor-service** container runs the same application code as Lambda, with an SQS polling loop replacing Lambda's event source mapping.
- A **Flask health endpoint** at `GET /health` enables Docker's health check mechanism.

### Why not use SAM Local or the Lambda Runtime Interface Emulator?

Both are viable alternatives. The custom polling approach was chosen because:

- It requires no additional tooling beyond Docker Compose.
- The health check endpoint can be tested directly.
- The same `process_user_activity` function is invoked in both environments, ensuring behavioral parity.

---

## 8. Observability

### Structured logging

All log entries are emitted as JSON objects with consistent fields:

- `timestamp`: ISO 8601 timestamp
- `level`: log severity
- `logger`: Python module name
- `correlationId`: unique ID per Lambda invocation (batch)
- `eventId`, `userId`, `messageId`: contextual identifiers

This format enables efficient querying in CloudWatch Logs Insights:

```
fields @timestamp, userId, eventId, @message
| filter level = "ERROR"
| sort @timestamp desc
```

### Correlation IDs

A UUID is generated at the start of each Lambda invocation and attached to every log entry within that batch. This allows tracing all processing steps for a single invocation across log entries.

---

## 9. Security Considerations

- **No hardcoded credentials**: All AWS credentials and endpoint URLs are loaded from environment variables.
- **Least privilege IAM**: The recommended IAM policy grants only the specific actions needed on the specific resources used.
- **Input validation**: Events are validated against a JSON schema before any processing occurs, preventing injection of unexpected data.
- **DLQ isolation**: Malformed messages are moved to a separate queue where they can be inspected without affecting the processing pipeline.

---

## 10. Scalability and Production Considerations

### Current design limits

- The notification threshold check and publish happen synchronously within the message processing loop. Under very high throughput, the SQS publish latency could become a bottleneck.
- The ProcessedEvents table receives a write for every event. At millions of events per day, this table could become expensive.

### Potential optimizations for scale

- **Batch SQS publishes**: Use `SendMessageBatch` to amortize network overhead across up to 10 messages.
- **FIFO output queue with deduplication**: Shift idempotency to the output side using SQS FIFO deduplication, reducing DynamoDB writes.
- **DynamoDB provisioned capacity with auto-scaling**: For predictable, sustained workloads.
- **Lambda reserved concurrency**: To control the degree of parallelism and protect downstream services from overload.
