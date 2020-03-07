[Back to main page](https://g-farrow.github.io/boto3_batch_utils)

---------------------------

# Unprocessed Items and Retries
Boto3 Batch Util's clients will attempt to send the records and messages submitted to it, to the AWS Services 
configured. However, in certain circumstances it may not be possible for the messages to be sent. It is useful to 
understand how the client will behave in these circumstances, e.g. if the principle is missing permissions needed to 
write to the target, network outage etc.

## Unprocessed Items
The most important thing to understand about error handling in the client, is that any messages which cannot be 
successfully processed will be returned by the `flush_payloads` method. The items will be returned in a list object.
They will be returned in the same state and structure as they were submitted.

Items are returned to the user so that they can be properly dealt with. For example:
* I am attempting to use the KinesisBatchDispatcher client to broadcast messages to a Kinesis stream.
* The Kinesis stream is throttling put requests.
* The KinesisBatchDispatcher will attempt to send the messages to the stream, but is only able to send half of them.
* When `flush_payloads` is called, the messages which failed to be sent will be returned.
* The unprocessed items can then be handled by way of a retry or a dead letter queue system.

## Retries
Although this logic is abstracted from the client, it us useful to understand the retry pattern which Boto3 Batch Utils
adopts in various circumstances:

### Boto3 ClientError
If the client is unable to broadcast an entire batch due to a Boto3 `ClientError` - often this is triggered by a 
throttled request - then it will attempt to retry the entire batch. It will attempt this a total of 5 times, after which
the payloads will be added to the `unprocessed_items` list.

### Partial Errors
Whilst a request to the AWS Service may be successful, it is possible that not all messages were accepted by the 
Service. In this case the response from the AWS Service will indicate which messages have been accepted or rejected.

Any messages rejected by the AWS Service will be retried by Boto3 Batch Utils. Each client type may handle this 
differently because the AWS Services they represent are not consistent. This may take the form of splitting batches into
smaller batches and retrying, continuing to break batches down until it gives up and places messages on the 
`unprocessed_items` list. Or it may switch from sending batches to using the AWS Service's unbatched/single payload API.

From the point of view of the interface, the retry mechanism is abstracted, but all clients will return any unprocessed
items in the `unprocessed_items` list, when `flush_payloads` is called.


---------------------------
[Back to main page](https://g-farrow.github.io/boto3_batch_utils)