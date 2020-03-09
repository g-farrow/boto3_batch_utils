[Back to main page](https://g-farrow.github.io/boto3_batch_utils)

---------------------------

# Dynamo
#### Abstracted Boto3 Methods:
* `batch_write_item()`

#### Usage
Batch write records to a DynamoDB table.

Initialise the client by providing the name of the DynamoDB Table and providing the `partition_key`. The `partition_key`
argument allows the client to find the Partition Key from within the submitted records and to deduplication payloads. If
the Partition Key is not present in the record or if it needs to be overridden, it can be submitted separately (see
Advanced Usage below).
```python
from boto3_batch_utils import DynamoBatchDispatcher


dy = DynamoBatchDispatcher('MyExampleDynamoTable', partition_key='id')

dy.submit_payload({'id': '', 'something': 'in', 'my': 'message'})

dy.flush_payloads()
```

## Advanced Usage
Using the basic features of the DynamoDB client will suit most use cases. However, in some scenarios you may need 
additional control over how the data is transmitted to DynamoDB. The DynamoDB client allows configuration of the
following behaviour:

### Batch Size
For information about batch sizes [click here](https://g-farrow.github.io/boto3_batch_utils/advanced-usage/limits).

The DynamoDB client has the following maximum batch limitations:

| *Limit Type*                 | *Limit*          |
|------------------------------|------------------|
| Number of Records            | 25               |
| Byte size of a single record | 400,000 bytes    |
| Byte size of a batch         | 16,000,000 bytes |

### Partition Key and Partition Key Location 
By default, the Partition Key will be the value located in the corresponding key within the payload when
`submit_payload` is called.

For instance, if the DynamoDB client is intialised with a `partition_key` of "myId" then `submit_payload` will look for
"myId" in the payload. This will then be used to ensure the payload is not a duplicate of a previously submitted 
payload.

However, it may be necessary to submit payloads which do not already contain the Partition Key attribute. In this 
scenario you can instruct Boto3 Batch Utils to locate a different value within the payload and set the Partition Key on 
your behalf.
```python
dy = DynamoBatchDispatcher('MyExampleDynamoTable', partition_key='myId')

dy.submit_payload({'id': 'abc123', 'something': 'in', 'my': 'message'}, partition_key_location='id')
```
In the above example the actual record submitted to DynamoDB would look as follows:
```python
{'myId': 'abc123', 'id': 'abc123', 'something': 'in', 'my': 'message'}
```
#### Partition Key Data Type
By default, using `partition_key_location` to identify the desired Partition Key will convert the value found to a 
`str`. If the DynamoDB table's Partition Key is _not_ a string you need to pass in the correct object type using
`partition_key_data_type` when the client is intialised, e.g.:
```python
dy = DynamoBatchDispatcher('MyExampleDynamoTable', partition_key='myId', partition_key_data_type=int)
```

### Sort Key
During initialisation, indicate that the DynamoDB table uses a Sort Key. This is done by passing in the location within
payloads that the Sort Key can be found. The Sort Key is used  by the client to assist with record deduplication.
```python
dy = DynamoBatchDispatcher('MyExampleDynamoTable', partition_key='myId', sort_key='createdDatetime')
```
> **Note**: Unlike partition key, the sort key can _not_ be overridden during the `submit_payload`. It must already 
> exist in the payload.

### Uniqueness
When a record is submitted to the DynamoDB client using `submit_payload` it is checked for uniqueness. The record will 
only be accepted if it is considered unique. Uniqueness criteria for this client are:
* If the DynamoDB table has a sort key: _The combination of the **Partition Key** and **Sort Key** must not exist in the 
list of records pending dispatch._
* If the DynamoDB table does not have a Sort Key: _The **Partition Key** must not exist in the list of records pending
dispatch._

---------------------------
[Back to main page](https://g-farrow.github.io/boto3_batch_utils)