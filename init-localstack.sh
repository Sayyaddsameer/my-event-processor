#!/bin/bash
set -euo pipefail

echo "Initializing LocalStack resources..."

REGION=us-east-1

# ---- SQS Queues ----

awslocal sqs create-queue \
  --queue-name user-activity-dlq \
  --region "$REGION"

DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/user-activity-dlq \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text \
  --region "$REGION")

awslocal sqs create-queue \
  --queue-name user-activity-input-queue \
  --region "$REGION" \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}"

awslocal sqs create-queue \
  --queue-name user-activity-output-queue \
  --region "$REGION"

# ---- DynamoDB Tables ----

awslocal dynamodb create-table \
  --table-name UserActivityAggregates \
  --attribute-definitions \
    AttributeName=userId,AttributeType=S \
    AttributeName=date,AttributeType=S \
  --key-schema \
    AttributeName=userId,KeyType=HASH \
    AttributeName=date,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION"

awslocal dynamodb create-table \
  --table-name ProcessedEvents \
  --attribute-definitions \
    AttributeName=eventId,AttributeType=S \
  --key-schema \
    AttributeName=eventId,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION"

awslocal dynamodb update-time-to-live \
  --table-name ProcessedEvents \
  --time-to-live-specification "Enabled=true,AttributeName=ttl" \
  --region "$REGION"

echo "LocalStack resources initialized successfully."
