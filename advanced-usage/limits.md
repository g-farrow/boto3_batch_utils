[Back to main page](https://g-farrow.github.io/boto3_batch_utils)

---------------------------

# Limits and Batch Management
Boto3 Batch Utils' clients follow a similar behaviour pattern. This is especially true for payload submission and for
batch management and limits.

### Submitting a Payload
When a payload is submitted to a client, it will be validated. Each client may have its own specific validation steps. 
However, all clients will ensure that a submitted payload does not exceed the byte size limit for the AWS Service:

| *Client/AWS Service* | *Max Bytes per Payload* |
|----------------------|-------------------------|
| Cloudwatch           | 40,960 bytes            |
| DynamoDB             | 400,000 bytes           |
| Kinesis              | 1,000,000 bytes         |
| SQS - Standard       | 262,144 bytes           |
| SQS - FIFO           | 262,144 bytes           | 

Submitting a payload which exceeds the client's payload byte size limit will result in a `ValueError` exception.

### Batches and Batch Management
Each client will manage the size of the batches it sends to its respective service. Each time a new payload is submitted
to the client it will decide when to send a batch based on the following criteria:

##### Maximum Batch Size
The client has a maximum batch size, or payload count. Each client may have a different maximum batch size:

| *Client/AWS Service* | *Max Payloads per Batch* |
|----------------------|--------------------------|
| Cloudwatch           | 150                      |
| DynamoDB             | 25                       |
| Kinesis              | 500                      |
| SQS - Standard       | 10                       |
| SQS - FIFO           | 10                       |

Each time a new payload is accepted to the client, the count of payloads is checked. If the count equals the client's
maximum, then the batch will be flushed - i.e. broadcast to the AWS Service. A new, empty, batch is then started.

If any payloads remain unsent in the client once processing is complete, they are flushed when `flush_payloads` is 
called. Therefore it is important that this method is called before the process/invocation ends.

##### Maximum Batch Byte Size
The client also has a maximum overall payload limit. Each client may have a different limit:

| *Client/AWS Service* | *Max Bytes per Batch* |
|----------------------|-----------------------|
| Cloudwatch           | 40,960 bytes          |
| DynamoDB             | 16,000,000 bytes      |
| Kinesis              | 5,000,000 bytes       |
| SQS - Standard       | 262,144 bytes         |
| SQS - FIFO           | 262,144 bytes         |

Each time a new payload is submitted, its byte size is checked. If the payload fits within the overall maximum limit for
a batch for this service, it is added to the batch. If it would cause the batch to exceed the limit, the existing batch
is flushed - i.e. broadcast to the AWS Service. The newly submitted payload is then added to a new, empty batch.



---------------------------
[Back to main page](https://g-farrow.github.io/boto3_batch_utils)