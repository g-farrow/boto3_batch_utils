[Back to main page](https://g-farrow.github.io/boto3_batch_utils)

# Kinesis
#### Abstracted Boto3 Methods:
* `put_records()`
* `put_record()`

#### Example
Batch Put items to a Kinesis stream
```python
from boto3_batch_utils import KinesisBatchDispatcher


kn = KinesisBatchDispatcher('MyExampleStreamName')

kn.submit_payload({"something": "in", "my": "message"})
kn.submit_payload({"tells": "me", "this": "is", "easy": True})

kn.flush_payloads()
```

## Advanced Usage
Using the basic features of the Kinesis client will suit most use cases. However, in some scenarios you may need 
additional control over how the data is transmitted to Kinesis. The Kinesis client allows configuration of the following
behaviour:

#### Batch Size
For information about batch sizes [click here](https://g-farrow.github.io/boto3_batch_utils/advanced-usage/batches).

The Kinesis client has the following maximum batch limitations:


[Back to main page](https://g-farrow.github.io/boto3_batch_utils)