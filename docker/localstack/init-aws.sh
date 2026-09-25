#!/bin/bash
set -euo pipefail

awslocal s3 mb s3://modaco-vendor-uploads || true

awslocal sqs create-queue --queue-name modaco-ingest-dlq >/dev/null
DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/modaco-ingest-dlq \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)

awslocal sqs create-queue --queue-name modaco-ingest-chunks \
  --attributes "{\"VisibilityTimeout\":\"360\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}" >/dev/null

awslocal sqs create-queue --queue-name modaco-s3-events \
  --attributes '{"VisibilityTimeout":"60"}' >/dev/null

awslocal s3api put-bucket-notification-configuration \
  --bucket modaco-vendor-uploads \
  --notification-configuration '{
    "QueueConfigurations": [{
      "Id": "uploads",
      "QueueArn": "arn:aws:sqs:us-east-1:000000000000:modaco-s3-events",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": "uploads/"}]}}
    }]
  }'

echo "localstack resources ready"
