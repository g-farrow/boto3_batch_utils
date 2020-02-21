import logging
from datetime import datetime

from boto3_batch_utils.Base import BaseDispatcher
from boto3_batch_utils.constants import CLOUDWATCH_BATCH_MAX_BYTES, CLOUDWATCH_BATCH_MAX_PAYLOADS,\
    CLOUDWATCH_MESSAGE_MAX_BYTES

logger = logging.getLogger('boto3-batch-utils')


def cloudwatch_dimension(name: str, value: (str, int)):
    """ Structure for forming aCloudwatch dimension """
    return {'Name': str(name), 'Value': str(value)}


class CloudwatchBatchDispatcher(BaseDispatcher):
    """
    Manage the batch 'put' of Cloudwatch metrics
    """

    def __init__(self, namespace: str, max_batch_size: int = 20, flush_payload_on_max_batch_size: bool = True):
        self.namespace = namespace
        super().__init__('cloudwatch', batch_dispatch_method='put_metric_data', max_batch_size=max_batch_size,
                         flush_payload_on_max_batch_size=flush_payload_on_max_batch_size)
        self._aws_service_batch_max_payloads = CLOUDWATCH_BATCH_MAX_PAYLOADS
        self._aws_service_message_max_bytes = CLOUDWATCH_MESSAGE_MAX_BYTES
        self._aws_service_batch_max_bytes = CLOUDWATCH_BATCH_MAX_BYTES
        self._batch_payload = {'Namespace': self.namespace, 'MetricData': []}
        self._validate_initialisation()

    def __str__(self):
        return f"CloudwatchBatchDispatcher::{self.namespace}"

    def _send_individual_payload(self, payload: dict, retry: int = 4):
        """ Send an individual metric to Cloudwatch """
        pass

    def _process_batch_send_response(self, response: dict):
        """ Process the response data from a batch put request (N/A) """
        pass

    def _batch_send_payloads(self, batch: dict = None, **kwargs):
        """ Attempt to send a single batch of metrics to Cloudwatch """
        if 'retry' in kwargs:
            super()._batch_send_payloads(batch, kwargs['retry'])
        else:
            super()._batch_send_payloads({'Namespace': self.namespace, 'MetricData': batch})

    def flush_payloads(self):
        """ Push all metrics in the payload list to Cloudwatch """
        super().flush_payloads()

    def _append_payload_to_current_batch(self, payload):
        """ Append the payload to the service specific batch structure """
        self._batch_payload['MetricData'].append(payload)

    def submit_payload(self, metric_name: str = None, timestamp: datetime = None, dimensions: (dict, list) = None,
                       value: (str, int) = None, unit: str = 'Count'):
        """ Submit a metric ready to be batched up and sent to Cloudwatch """
        payload = {
                'MetricName': metric_name,
                'Timestamp': timestamp or datetime.now(),
                'Value': value,
                'Unit': unit
            }
        logger.debug(f"Payload submitted to {self.aws_service_name} dispatcher: {payload}")
        if dimensions:
            payload['Dimensions'] = dimensions if isinstance(dimensions, list) else [dimensions]
        super().submit_payload(payload)
