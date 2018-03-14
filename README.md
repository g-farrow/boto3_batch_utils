Boto3 Batch Utils
=================
This library offers some functionality to assist in writing records to AWS services in batches, where your data is not naturally batched. This helps to achieve significant efficiencies when interacting with those AWS services as batch writes are often times orders of magnitude faster than individual writes.

## General Usage
The library is very simple to use and works on the concept that you wish to submit payloads to a dispatcher, and not care too much about how to batch up the payloads and send them into their target service. This allows you to utilise the significant efficiencies of patch send/put/write methods, without the problem of error handling and batch sizes.

Each of the supported services has it's own dispatcher class. Each has the same 2 meths with which to interact:
* **submit_payload**: pass in a payload (e.g. a single message, metric etc)
* **flush_payloads**: send all payloads in the backlog. Don't worry if there are a lot of payloads, the dispatcher will automatically break them down into chunks. 

*It is recommended you `flush_payloads` every invocation or when payload lists become long (100-200 items +) to avoid issues.*

## Supported Services

#### Kinesis Put
Batch Put items to a Kinesis stream
```python
from boto3_batch_utils import KinesisBatchDispatcher
kn = KinesisBatchDispatcher('MyExampleStreamName')
kn.submit_payload({"something": "in", "my": "message"})
kn.submit_payload({"tells": "me", "this": "is", "easy": True})
kn.flush_payloads()
```

#### Dynamo Write
Batch write records to a DynamoDB table
```python
from boto3_batch_utils import DynamoBatchDispatcher
dy = DynamoBatchDispatcher('MyExampleDynamoTable', primary_partition_key='Id')
dy.submit_payload({"something": "in", "my": "message"})
dy.submit_payload({"tells": "me", "this": "is", "easy": True})
dy.flush_payloads()
```

#### Cloudwatch Put Metrics
Batch put metric data to Cloudwatch. CLoudwatch comes with a handy dimension builder function `cloudwatch_dimension` to help you construct dimensions
```python
from boto3_batch_utils import CloudwatchBatchDispatcher, cloudwatch_dimension
cw = CloudwatchBatchDispatcher('TEST_JUNK')
cw.submit_payload('RANDOM_METRIC', dimensions=cloudwatch_dimension('dimA', '12345'), value=555, unit='Count')
cw.submit_payload('RANDOM_METRIC', dimensions=cloudwatch_dimension('dimA', '12345'), value=1234, unit='Count')
cw.flush_payloads()
```

#### SQS Send Messages
Batch send messages to an SQS queue
```python
from boto3_batch_utils import SQSBatchDispatcher
sqs = SQSBatchDispatcher("aQueueWithAName")
sqs.submit_payload("some message of some sort")
sqs.submit_payload("a different message, probably a similar sort")
sqs.flush_payloads()
```