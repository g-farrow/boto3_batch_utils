import logging
from datetime import datetime

from boto3_batch_utils.Base import BaseDispatcher


logger = logging.getLogger()

cloudwatch_batch_limit = 25


def cloudwatch_dimension(name=None, value=None):
    """ Structure for forming aCloudwatch dimension """
    return {'Name': name, 'Value': value}


class CloudwatchBatchDispatcher(BaseDispatcher):
    """
    Manage the batch 'put' of Cloudwatch metrics
    """

    def __init__(self, namespace, max_batch_size=cloudwatch_batch_limit, flush_payload_on_max_batch_size=True):
        self.namespace = namespace
        super().__init__('cloudwatch', 'put_metric_data', batch_size=max_batch_size,
                         flush_payload_on_max_batch_size=flush_payload_on_max_batch_size)

    def _send_individual_payload(self, payload, retry=4):
        """ Send an individual metric to Cloudwatch """
        pass

    def _process_batch_send_response(self, response):
        """ Process the response data from a batch put request (N/A) """
        pass

    def _batch_send_payloads(self, batch=None, **nested_batch):
        """ Attempt to send a single batch of metrics to Cloudwatch """
        super()._batch_send_payloads({'Namespace': self.namespace, 'MetricData': batch})

    def flush_payloads(self):
        """ Push all metrics in the payload list to Cloudwatch """
        super().flush_payloads()

    def submit_payload(self, metric_name=None, timestamp=datetime.now(), dimensions=None, value=None, unit=None):
        """ Submit a metric ready to be batched up and sent to Cloudwatch """
        metric = {
                'MetricName': metric_name,
                'Dimensions': dimensions,
                'Timestamp': timestamp,
                'Value': value,
                'Unit': unit
            }
        super().submit_payload(metric)
