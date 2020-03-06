[Back to main page](https://g-farrow.github.io/boto3_batch_utils)

---------------------------

# Dynamo
#### Abstracted Boto3 Methods:
* `batch_write_item()`

#### Usage
Batch write records to a DynamoDB table.

Initialise the client by providing the name of the DynamoDB Table and providing the `partition_key`. The `partition_key`
argument allows the client to find the Partition Key from within the submitted records. If the Partition Key is not
present in the record, it can be submitted separately (see Advanced Usage below).
```python
from boto3_batch_utils import DynamoBatchDispatcher


dy = DynamoBatchDispatcher('MyExampleDynamoTable', partition_key='id')

dy.submit_payload({'id': '', 'something': 'in', 'my': 'message'})
dy.submit_payload({'id': '', 'tells': 'me', 'this': 'is', 'easy': True})

dy.flush_payloads()
```

## Advanced Usage
Using the basic features of the DynamoDB client will suit most use cases. However, in some scenarios you may need 
additional control over how the data is transmitted to DynamoDB. The DynamoDB client allows configuration of the
following behaviour:

#### Batch Size
For information about batch sizes [click here](https://g-farrow.github.io/boto3_batch_utils/advanced-usage/batches).

The DynamoDB client has the following maximum batch limitations:

| *Limit Type*                 | *Limit*          |
|------------------------------|------------------|
| Number of Records            | 25               |
| Byte size of a single record | 400,000 bytes    |
| Byte size of a batch         | 16,000,000 bytes |

#### Partition Key
# TODO if the partition key is not present in the record

##### Partition Key Data Type
# TODO

#### Sort Key


---------------------------
[Back to main page](https://g-farrow.github.io/boto3_batch_utils)