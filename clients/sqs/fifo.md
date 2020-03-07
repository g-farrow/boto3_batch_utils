[Back to main page](https://g-farrow.github.io/boto3_batch_utils)

---------------------------

## SQS FIFO Queues
#### Abstracted Boto3 Methods:
* `send_message_batch`
* `send_message`

#### Usage
Batch send messages to an SQS "FIFO" queue. 
([Click here for SQS Standard Queues](https://g-farrow.github.io/boto3_batch_utils/clients/sqs/standard))

The SQS FIFO Client is initialised with the name of the SQS queue. Once initialised, messages can be submitted as 
`dict` objects.

```python
from boto3_batch_utils import SQSFifoBatchDispatcher


sqs = SQSFifoBatchDispatcher("aQueueWithAName")

payload = {'myId': 'abc123', 'something': 'in', 'my': 'message'}

sqs.submit_payload(payload)

sqs.flush_payloads()
```

## Advanced Usage
Using the basic features of the SQS FIFO client will suit most use cases. However, in some scenarios you may need 
additional control over how the data is transmitted to SQS. The SQS FIFO client allows configuration of the following
behaviour:

#### Batch Size
For information about batch sizes [click here](https://g-farrow.github.io/boto3_batch_utils/advanced-usage/limits).

The Sqs FIFO client has the following maximum batch limitations:

| *Limit Type*                 | *Limit*        |
|------------------------------|----------------|
| Number of Records            | 10             |
| Byte size of a single record | 262,144 bytes  |
| Byte size of a batch         | 262,144 bytes  |

#### Message Deduplication 
SQS FIFO Queues provide 'exactly once delivery' functionality. This is achieved partly through message deduplication.
You can learn about message deduplication here: 
[Using the Amazon SQS Message Deduplication ID](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagededuplicationid-property.html)

##### Message Deduplication Id
By default Boto3 Batch Utils will create and assign a Message Deduplication Id automatically. However, in some 
circumstances it may be useful to provide an override for this default. A Message Deduplication Id can be provided by
using the `message_deduplication_id` argument on the `submit_payload` method:
 ```python
sqs.submit_payload(payload, message_id="abc123", message_deduplication_id='cde123')
sqs.submit_payload(payload, message_id="abc123", message_deduplication_id=payload['myId'])
```

#### Message Id
Within a single batch submission to SQS, each message must be assigned a unique ID. By default the Boto3 Batch Utils 
client will assign a unique ID for you. However, if you wish to override this, simply provide the desired ID using the
`message_id` argument on the `submit_payload` method:
```python
sqs.submit_payload(payload, message_id="abc123")
sqs.submit_payload(payload, message_id=payload['myId'])
```

#### Message Group Id 
Messages submitted to an SQS FIFO Queue are deduplicated and ordered within a Message Group. To learn more about how 
this works, read the 
[official documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagegroupid-property.html)

By default Boto3 Batch Utils will use a `message_group_id` of 'unset' - ie. all messages submitted via the client will
be sent to the same Message Group. This can be easily overridden, simply provide the `message_group_id` as an argument
on the `submit_payload` method:
 ```python
sqs.submit_payload(payload, message_id="abc123", message_group_id='mg1A')
```

#### Uniqueness
When a record is submitted to the SQS FIFO client using `submit_payload` it is checked for uniqueness. The record
will only be accepted if it is considered unique. Uniqueness criteria for this client are:
* The message's `message_deduplication_id` does not match a `message_deduplication_id` of any of the messages in the 
list of records pending dispatch.
> *Note*: This is different to the SQS _Standard_ client which instead uses a message's `message_id`.

For more information, refer to the **Message Deduplication Id** section above.


---------------------------
[Back to main page](https://g-farrow.github.io/boto3_batch_utils)