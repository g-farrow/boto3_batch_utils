import logging
from datetime import datetime

from boto3_batch_utils.Base import BaseDispatcher


logger = logging.getLogger('boto3-batch-utils')


def cloudwatch_dimension(name, value):
    """ Structure for forming aCloudwatch dimension """
    return {'Name': str(name), 'Value': str(value)}


class CloudwatchBatchDispatcher(BaseDispatcher):
    """
    Manage the batch 'put' of Cloudwatch metrics
    """

    def __init__(self, namespace, max_batch_size=20, flush_payload_on_max_batch_size=True):
        self.namespace = namespace
        super().__init__('cloudwatch', 'put_metric_data', batch_size=max_batch_size,
                         flush_payload_on_max_batch_size=flush_payload_on_max_batch_size)

    def _send_individual_payload(self, payload, retry=4):
        """ Send an individual metric to Cloudwatch """
        pass

    def _process_batch_send_response(self, response):
        """ Process the response data from a batch put request (N/A) """
        pass

    def _batch_send_payloads(self, batch=None, **kwargs):
        """ Attempt to send a single batch of metrics to Cloudwatch """
        if 'retry' in kwargs:
            super()._batch_send_payloads(batch, kwargs['retry'])
        else:
            super()._batch_send_payloads({'Namespace': self.namespace, 'MetricData': batch})

    def flush_payloads(self):
        """ Push all metrics in the payload list to Cloudwatch """
        super().flush_payloads()

    def submit_payload(self, metric_name=None, timestamp=None, dimensions=None, value=None, unit='Count'):
        """ Submit a metric ready to be batched up and sent to Cloudwatch """
        payload = {
                'MetricName': metric_name,
                'Timestamp': timestamp or datetime.now(),
                'Value': value,
                'Unit': unit
            }
        logger.debug("Payload submitted to {} dispatcher: {}".format(self._subject_name, payload))
        if dimensions:
            payload['Dimensions'] = dimensions if isinstance(dimensions, list) else [dimensions]
        super().submit_payload(payload)
