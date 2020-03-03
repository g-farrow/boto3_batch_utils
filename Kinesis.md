layout: page
title: Kinesis
permalink: /kinesis/

# Kinesis
## Abstracted Boto3 Methods:
* `put_records()`
* `put_record()`

## Example
Batch Put items to a Kinesis stream
```python
from boto3_batch_utils import KinesisBatchDispatcher


kn = KinesisBatchDispatcher('MyExampleStreamName')

kn.submit_payload({"something": "in", "my": "message"})
kn.submit_payload({"tells": "me", "this": "is", "easy": True})

kn.flush_payloads()
```