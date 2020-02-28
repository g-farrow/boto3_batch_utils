import logging
from datetime import datetime

from boto3_batch_utils.Base import BaseDispatcher
from boto3_batch_utils import constants


logger = logging.getLogger('boto3-batch-utils')


def cloudwatch_dimension(name: str, value: (str, int)):
    """ Structure for forming aCloudwatch dimension """
    return {'Name': str(name), 'Value': str(value)}


class CloudwatchBatchDispatcher(BaseDispatcher):
    """
    Manage the batch 'put' of Cloudwatch metrics
    """

    def __init__(self, namespace: str, max_batch_size: int = 20):
        self.namespace = namespace
        super().__init__('cloudwatch', batch_dispatch_method='put_metric_data', max_batch_size=max_batch_size)
        self._aws_service_batch_max_payloads = constants.CLOUDWATCH_BATCH_MAX_PAYLOADS
        self._aws_service_message_max_bytes = constants.CLOUDWATCH_MESSAGE_MAX_BYTES
        self._aws_service_batch_max_bytes = constants.CLOUDWATCH_BATCH_MAX_BYTES
        self._batch_payload_wrapper = {'Namespace': self.namespace, 'MetricData': []}
        self._batch_payload = []
        self._validate_initialisation()

    def __str__(self):
        return f"CloudwatchBatchDispatcher::{self.namespace}"

    def submit_payload(self, metric_name: str, value: (str, int), timestamp: datetime = None,
                       dimensions: (dict, list) = None, unit: str = 'Count'):
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

    def _batch_send_payloads(self, batch: dict = None, **kwargs):
        """ Attempt to send a single batch of metrics to Cloudwatch """
        if 'retry' in kwargs:
            super()._batch_send_payloads(batch, kwargs['retry'])
        else:
            super()._batch_send_payloads({'Namespace': self.namespace, 'MetricData': batch})
