Boto3 Batch Utils
=================

![PyPI](https://img.shields.io/pypi/v/boto3-batch-utils?style=for-the-badge)
![GitHub Workflow Status (branch)](https://img.shields.io/github/workflow/status/g-farrow/boto3_batch_utils/Master%20Pipeline?label=MASTER%20BRANCH&logo=github&style=for-the-badge)

This library offers some functionality to assist in writing records to AWS services in batches, where your data is not 
naturally batched. This helps to achieve significant efficiencies when interacting with those AWS services as batch 
writes are often much more efficient than individual writes.

# Installation
The library can be installed using `pip`:
```
pip install boto3-batch-utils
```

You may install a specific version of the library:
```
pip install boto3-batch-utils==3.0.0
```

### Boto3 and Configuration
Boto3 Batch Utils is an abstraction around AWS' Boto3 library. `boto3` is a dependency and will be installed 
automatically, if it is not already present.

You will need to configure your AWS credentials and roles in exactly the same way as you would if using `boto3`
directly.

For more information on `boto3` configuration, refer to the AWS documentation 
[here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html).

# Concepts
The library is very simple to use. To use it, you must initialise a client, send it the payloads you want to transmit
 and finally tell the client to clear down.

To use the library you do not need care how to batch up the payloads and send them into their target service. The 
library will take care of this for you. This allows you to utilise the significant efficiencies of `boto3`'s batch 
send/put/write methods, without the headaches of error handling and batch sizes.

Each of the supported services has it's own dispatcher client. Each has the same 2 methods with which to interact. So
interacting with each of the various service clients is similar and follows the same 3 steps: 

**Initialise**:
Instantiate the batch dispatcher, passing in the required configuration. e.g. 
> `sqs_client = SQSBatchDispatcher("MySqsQueue")`

**submit_payload**:
Pass in a payload (e.g. a single message, metric etc): e.g.
> `sqs_client.submit_payload({'test': 'message'})`

**flush_payloads**:
Send all payloads in the backlog. e.g.
> `unprocessed_items = sqs_client.flush_payloads()`

> If you are using `boto3-batch-utils` in AWS Lambda, you should call `flush_payloads` at the end of every 
invocation.

# Supported Services
The following AWS Services are currently supported by Boto3 Batch Utils:

#### [Kinesis](https://g-farrow.github.io/boto3_batch_utils/clients/kinesis)
#### [DynamoDB](https://g-farrow.github.io/boto3_batch_utils/clients/dynamodb)
#### [Cloudwatch](https://g-farrow.github.io/boto3_batch_utils/clients/cloudwatch)
#### [SQS Standard](https://g-farrow.github.io/boto3_batch_utils/clients/sqs/standard)
#### [SQS Fifo](https://g-farrow.github.io/boto3_batch_utils/clients/sqs/fifo)

----------------------------

# Advanced Usage
The library can be used very simply. However in some scenarios you may require greater control over its behaviour. Or
to understand how it works in more detail.


#### Limits and Batch Management
Learn how Boto3 Batch Utils decides when to dispatch batches and adhere to AWS Service limits in
[Limits and Batch Management](https://g-farrow.github.io/boto3_batch_utils/advanced-usage/limits)

#### Unprocessed Items
What happens if the client is unable to send a payload or an entire batch? Learn how Boto3 Batch Utils handles errors
and failures in [Unprocessed Items](https://g-farrow.github.io/boto3_batch_utils/advanced-usage/unprocessed-items)

#### Client Specific Advanced Usage
Each client has its own advanced usage, refer to each client's docs for further information.