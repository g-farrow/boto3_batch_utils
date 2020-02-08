import logging
import boto3
from botocore.exceptions import ClientError

from boto3_batch_utils.utils import chunks


logger = logging.getLogger('boto3-batch-utils')

_boto3_interface_type_mapper = {
    'dynamodb': 'resource',
    'kinesis': 'client',
    'sqs': 'client',
    'cloudwatch': 'client'
}


class BaseDispatcher:

    def __init__(self, subject, batch_dispatch_method, individual_dispatch_method=None, batch_size=1,
                 flush_payload_on_max_batch_size=True):
        """
        :param subject: object - the boto3 client which shall be called to dispatch each payload
        :param batch_dispatch_method: method - the method to be called when attempting to dispatch multiple items in a
        payload
        :param individual_dispatch_method: method - the method to be called when attempting to dispatch an individual
        item to the subject
        :param batch_size: int - Maximum size of a payload batch to be sent to the target
        :param flush_payload_on_max_batch_size: bool - should payload be automatically sent once the payload size is
        equal to that of the maximum permissible batch (True), or should the manager wait for a flush payload call
        (False)
        """
        self._subject_name = subject
        self._subject = getattr(boto3, _boto3_interface_type_mapper[self._subject_name])(self._subject_name)
        self._batch_dispatch_method = getattr(self._subject, str(batch_dispatch_method))
        if individual_dispatch_method:
            self._individual_dispatch_method = getattr(self._subject, individual_dispatch_method)
        else:
            self._individual_dispatch_method = None
        self.max_batch_size = batch_size
        self.flush_payload_on_max_batch_size = flush_payload_on_max_batch_size
        self._payload_list = []
        logger.debug("Batch dispatch manager initialised: {}".format(self._subject_name))

    def _send_individual_payload(self, payload, retry=4):
        """ Send an individual payload to the subject """
        logger.debug("Attempting to send individual payload ({} retries left): {}".format(retry, payload))
        try:
            if isinstance(payload, dict):
                logger.debug("Submitting payload as keyword args")
                self._individual_dispatch_method(**payload)
            else:
                logger.debug("Submitting payload as arg")
                self._individual_dispatch_method(payload)
        except ClientError as e:
            if retry:
                logger.debug("Individual send attempt has failed, retrying")
                self._send_individual_payload(payload, retry-1)
            else:
                logger.error("Individual send attempt has failed, no more retries remaining")
                raise

    def _process_batch_send_response(self, response):
        """ Process the response data from a batch put request """
        pass

    def _batch_send_payloads(self, batch, retry=4):
        """ Attempt to send a single batch of payloads to the subject """
        logger.debug("Sending batch type {} payloads to {}".format(type(batch), self._subject_name))
        try:
            if isinstance(batch, dict):
                response = self._batch_dispatch_method(**batch)
                self._process_batch_send_response(response)
            else:
                response = self._batch_dispatch_method(batch)
                self._process_batch_send_response(response)
            logger.debug("Batch send response: {}".format(response))
        except ClientError as e:
            if retry > 0:
                logger.warning("{} batch send has caused an error, retrying to send ({} retries remaining): {}".format(
                    self._subject_name, retry, str(e)))
                logger.debug("Failed batch: (type: {}) {}".format(type(batch), batch))
                self._batch_send_payloads(batch, retry=retry-1)
            else:
                raise

    def flush_payloads(self):
        """ Push all payloads in the payload list to the subject """
        logger.debug("{} payload list has {} entries".format(self._subject_name, len(self._payload_list)))
        if self._payload_list:
            logger.debug("Preparing to send {} records to {}".format(len(self._payload_list), self._subject_name))
            batch_list = list(chunks(self._payload_list, self.max_batch_size))
            logger.debug("Payload list split into {} batches".format(len(batch_list)))
            for batch in batch_list:
                self._batch_send_payloads(batch)
            self._payload_list = []
        else:
            logger.info("No payloads to flush to {}".format(self._subject_name))

    def _flush_payload_selector(self):
        """ Decide whether or not to flush the payload (usually used following a payload submission) """
        logger.debug("Payload list now contains '{}' payloads, max batch size is '{}'".format(
            len(self._payload_list), self.max_batch_size
        ))
        if self.flush_payload_on_max_batch_size and len(self._payload_list) >= self.max_batch_size:
            logger.debug("Max batch size has been reached, flushing the payload list contents")
            self.flush_payloads()
        else:
            logger.debug("Max batch size of {} for {} has not yet been reached, continuing".format(self.max_batch_size,
                                                                                                   self._subject_name))

    def submit_payload(self, payload):
        """ Submit a metric ready to be batched up and sent to Cloudwatch """
        self._payload_list.append(payload)
        logger.debug("Payload has been added to the {} dispatcher payload list: {}".format(self._subject_name, payload))
        self._flush_payload_selector()

