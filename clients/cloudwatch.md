[Back to main page](https://g-farrow.github.io/boto3_batch_utils)

---------------------------

# Cloudwatch
### Abstracted Boto3 Methods:
* `put_metric_data()`

### Usage
Batch put metric data to Cloudwatch.

The Cloudwatch client is intialised with the 
[Namespace](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html#Namespace) required.

To submit a custom Cloudwatch metric, provide the following information:
* `metric_name`: The name of the metric which the data will be associated with. See 
[Metrics](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html#Metric) for more 
information.
* `value`: The value of the metric.
* `timestamp`: (Optional) Defaults to `now()`. The timestamp for when the metric is pertinent.
* `dimensions`: (Optional) See 
[Dimensions](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html#Dimension) for more
information. Provide either a `cloudwatch_dimension` or a list of multiple `cloudwatch_dimension`.
* `unit`: (Optional) Defaults to `Count`. For more information see 
[Metric Datum](https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricDatum.html). Any of the 
following values  may be provided: `Seconds | Microseconds | Milliseconds | Bytes | Kilobytes | Megabytes | Gigabytes | 
Terabytes | Bits | Kilobits | Megabits | Gigabits | Terabits | Percent | Count | Bytes/Second | Kilobytes/Second | 
Megabytes/Second | Gigabytes/Second | Terabytes/Second | Bits/Second | Kilobits/Second | Megabits/Second | 
Gigabits/Second | Terabits/Second | Count/Second | None`

```python
from boto3_batch_utils import CloudwatchBatchDispatcher


cw = CloudwatchBatchDispatcher('TestService')

cw.submit_payload('DoingACountMetric', value=555)
cw.submit_payload('DoingACountMetric', value=4, unit='Seconds')

cw.flush_payloads()
```

Cloudwatch comes with a handy dimension builder function `cloudwatch_dimension` 
to help you construct dimensions (see below).

## Advanced Usage
The Cloudwatch client can be used very simply. However, you can gain additional control with the following advanced 
features:

### Batch Size
For information about batch sizes [click here](https://g-farrow.github.io/boto3_batch_utils/advanced-usage/limits).

The Cloudwatch client has the following maximum batch limitations:

| *Limit Type*                 | *Limit*         |
|------------------------------|-----------------|
| Number of Records            | 150             |
| Byte size of a single record | 40,960 bytes    |
| Byte size of a batch         | 40,960 bytes    |
| Dimensions Per Metric        | 10              |

### Dimensions
[Cloudwatch Metrics Dimensions](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html#Dimension)
 are optional. If required between one and ten dimensions may be used. Boto3 Batch Utils provides a helper function
 `cloudwatch_dimension` to correctly format dimensions. The inputs to this helper function are:
 * `name`: The name of the dimension.
 * `value`: The value of the dimension.
 
 ```python
from boto3_batch_utils import CloudwatchBatchDispatcher, cloudwatch_dimension as cd


cw = CloudwatchBatchDispatcher('TestService')

cw.submit_payload('DoingACountMetric', value=1234, dimensions=cd('dimA', '12345'))
cw.submit_payload('DoingASecondsMetric', value=978, dimensions=[cd('timeA', '11'), cd('timeB', '4')])

cw.flush_payloads()
```

### Uniqueness
When a record is submitted to the Cloudwatch client using `submit_payload` it is *NOT* checked for uniqueness. When 
sending data to Cloudwatch Metrics Stream, it is both likely and valid to send duplicate metric payloads.

---------------------------
[Back to main page](https://g-farrow.github.io/boto3_batch_utils)