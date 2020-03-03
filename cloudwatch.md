layout: page
title: "Cloudwatch"
permalink: /cloudwatch2

# Cloudwatch
## Abstracted Boto3 Methods:
* `put_metric_data()`

## Example
Batch put metric data to Cloudwatch. Cloudwatch comes with a handy dimension builder function `cloudwatch_dimension` 
to help you construct dimensions
```python
from boto3_batch_utils import CloudwatchBatchDispatcher, cloudwatch_dimension


cw = CloudwatchBatchDispatcher('TestService')

cw.submit_payload('DoingACountMetric', dimensions=cloudwatch_dimension('dimA', '12345'), value=555, unit='Count')
cw.submit_payload('DoingACountMetric', dimensions=cloudwatch_dimension('dimA', '12345'), value=1234, unit='Count')

cw.flush_payloads()
```