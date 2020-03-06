[Back to main page](https://g-farrow.github.io/boto3_batch_utils)
---------------------------

# SQS Standard Queues
#### Abstracted Boto3 Methods:
* `send_message_batch`
* `send_message`

#### Usage
Batch send messages to an SQS "Standard" queue. 
([Click here for SQS FIFO Queues](https://g-farrow.github.io/boto3_batch_utils/clients/sqs/fifo))

The SQS Standard Client is initialised with the name of the SQS queue. Once initialised, messages can be submitted as 
`dict` objects.


```python
from boto3_batch_utils import SQSBatchDispatcher


sqs = SQSBatchDispatcher('aQueueWithAName')

payload = {'something': 'in', 'my': 'message'}

sqs.submit_payload(payload)

sqs.flush_payloads()
```

## Advanced Usage
Using the basic features of the SQS Standard client will suit most use cases. However, in some scenarios you may need 
additional control over how the data is transmitted to SQS. The SQS Standard client allows configuration of the following
behaviour:

#### Message Id
Within a single batch submission to SQS, each message must be assigned a unique ID. By default the Boto3 Batch Utils 
client will assign a unique ID for you. However, if you wish to override this, simply provide the desired ID using the
`message_id` argument on the `submit_payload` method:
```python
sqs.submit_payload(payload, message_id="abc123")
```

#### Delay Seconds
SQS allows messages to be published to a Standard queue, but to remain invisible for a period of time. Read more about
this functionality in the 
[official AWS docs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-delay-queues.html).

> Please note that this feature is not available on FIFO queues.

Boto3 Batch Utils allows configuration of Delay Seconds on a per message basis. The delay maybe 0 to 900 seconds 
(15 minutes). To use this feature, provide the `delay_seconds` argument to the `submit_payload` method:
```python
sqs.submit_payload(payload, delay_seconds=45)
```

---------------------------
[Back to main page](https://g-farrow.github.io/boto3_batch_utils)