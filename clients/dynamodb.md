[Back to main page](https://g-farrow.github.io/boto3_batch_utils)
---------------------------

# Dynamo
#### Abstracted Boto3 Methods:
* `batch_write_item()`

#### Usage
Batch write records to a DynamoDB table
```python
from boto3_batch_utils import DynamoBatchDispatcher


dy = DynamoBatchDispatcher('MyExampleDynamoTable', partition_key='Id')

dy.submit_payload({"something": "in", "my": "message"})
dy.submit_payload({"tells": "me", "this": "is", "easy": True})

dy.flush_payloads()
```

---------------------------
[Back to main page](https://g-farrow.github.io/boto3_batch_utils)