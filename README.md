Boto3 Batch Utils
=================
This library offers some functionality to assist in writing records to AWS services in batches, where your data is not naturally batched. This helps to achieve significant efficiencies when interacting with those AWS services as batch writes are often times orders of magnitude faster than individual writes.

# Supported Services

## Kinesis Put
Batch Put items to a Kinesis stream

## Dynamo Write
Batch write records to a DynamoDB table

## Cloudwatch Put Metrics
Batch put metric data to Cloudwatch

## SQS Send Messages
Batch send messages to an SQS queue