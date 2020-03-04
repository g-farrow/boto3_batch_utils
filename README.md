![PyPI](https://img.shields.io/pypi/v/boto3-batch-utils?style=for-the-badge)
![GitHub Workflow Status (branch)](https://img.shields.io/github/workflow/status/g-farrow/boto3_batch_utils/Master%20Pipeline?label=MASTER%20BRANCH&logo=github&style=for-the-badge)


Boto3 Batch Utils
=================
This library offers some functionality to assist in writing records to AWS services in batches, where your data is not 
naturally batched. This helps to achieve significant efficiencies when interacting with those AWS services as batch 
writes are often much more efficient than individual writes.

[Documentation]()

# Installation
The package can be installed using `pip`:
```
pip install boto3-batch-utils
```

You may install a specific version of the package:
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

To use the package you do not need care how to batch up the payloads and send them into their target service. The 
package will take care of this for you. This allows you to utilise the significant efficiencies of `boto3`'s batch 
send/put/write methods, without the headaches of error handling and batch sizes.

Each of the supported services has it's own dispatcher client. Each has the same 2 methods with which to interact. So
interacting with each of the various service clients is similar and follows the same 3 steps: 
* **Initialise**: Instantiate the batch dispatcher, passing in the required configuration.
* **submit_payload**: pass in a payload (e.g. a single message, metric etc).
* **flush_payloads**: send all payloads in the backlog.

> If you are using `boto3-batch-utils` in AWS Lambda, you should call `.flush_payloads()` at the end of every 
invocation.

# Documentation

Full documentation is available here: [boto3-batch-utils Docs](https://g-farrow.github.io/boto3_batch_utils/)
