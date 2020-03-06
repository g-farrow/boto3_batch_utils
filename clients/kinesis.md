[Back to main page](https://g-farrow.github.io/boto3_batch_utils)

---------------------------

# Kinesis
#### Abstracted Boto3 Methods:
* `put_records()`
* `put_record()`

#### Usage
Batch Put items to a Kinesis stream.

Initialise the client by providing it with the name of the Kinesis Stream.

Submit the records which should be sent.

Finally, flush any remaining, unsent payloads.
```python
from boto3_batch_utils import KinesisBatchDispatcher


kn = KinesisBatchDispatcher('MyExampleStreamName')

kn.submit_payload({'something': 'in', 'my': 'message'})
kn.submit_payload({'tells': 'me', 'this': 'is', 'easy': True})

kn.flush_payloads()
```

## Advanced Usage
Using the basic features of the Kinesis client will suit most use cases. However, in some scenarios you may need 
additional control over how the data is transmitted to Kinesis. The Kinesis client allows configuration of the following
behaviour:

#### Batch Size
For information about batch sizes [click here](https://g-farrow.github.io/boto3_batch_utils/advanced-usage/batches).

The Kinesis client has the following maximum batch limitations:

| *Limit Type*                 | *Limit*         |
|------------------------------|-----------------|
| Number of Records            | 500             |
| Byte size of a single record | 1,000,000 bytes |
| Byte size of a batch         | 5,000,000 bytes |

#### Partition Keys
A Kinesis stream is made up of 1 or more _Shards_. For more information about Shards, refer to thew Kinesis docs 
[here](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html). By default Boto3 Batch Utils will evenly
distribute records across all Shards in a Stream. It does this by assigning a random unique id to each record, which is 
used as that records [Partition Key](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html#partition-key).
If your Kinesis Stream has multiple Shard purely as a means of achieving greater scale, the default will work very 
nicely.

However, you may require more control over the allocation of a record to a given Shard. This can be done by telling
the client which part of a record to use as the Partition Key value.

##### Example
In this example, records can have varying content, keys and values. However, all records will have a unique id. In this
case I wish to use this id as the Partition Key. The id is located against the 'Id' key in the record:
```python
record = {'id': 'a4f315f8ef6b40d9bfdc3837fa6c0d64', 'type': 'furniture', 'name': 'table', 'legs': {'count': 4,
'material': 'wood'}}
``` 
> Note: The attribute to be used as the Partition Key must be at the top level of the record's hierarchy.

In order to configure the client, pass in the optional `partition_key_identifier` argument. Provide the key name of the
key to be used:
```python
kn_client = KinesisBatchDispatcher('MyExampleStreamName', partition_key_identifier='id')
```
With this configuration the example record would be dispatched to Kinesis with in the following payload structure:
```python
{
'Data': {'id': 'a4f315f8ef6b40d9bfdc3837fa6c0d64', 'type': 'furniture', 'name': 'table', 'legs': {'count': 4,
    'material': 'wood'}},
'PartitionKey': 'a4f315f8ef6b40d9bfdc3837fa6c0d64'
}
```

---------------------------

[Back to main page](https://g-farrow.github.io/boto3_batch_utils)