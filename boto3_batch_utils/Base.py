import logging
import boto3
from botocore.exceptions import ClientError

from boto3_batch_utils.utils import chunks


logger = logging.getLogger()


class BaseDispatcher:

    def __init__(self, subject, batch_dispatch_method, individual_dispatch_method, batch_size=1,
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
        self.subject_name = subject
        self.subject = boto3.client(self.subject_name)
        self.batch_dispatch_method = getattr(self.subject, str(batch_dispatch_method))
        self.individual_dispatch_method = getattr(self.subject, individual_dispatch_method)
        self.max_batch_size = batch_size
        self.flush_payload_on_max_batch_size = flush_payload_on_max_batch_size
        self.payload_list = []
        logger.debug("Batch dispatch manager initialised: {}".format(self.subject_name))

    def _send_individual_payload(self, payload, retry=4):
        """ Send an individual payload to the subject """
        logger.debug("Attempting to send individual payload ({} retries left)".format(retry))
        try:
            if isinstance(payload, dict):
                self.individual_dispatch_method(**payload)
            else:
                self.individual_dispatch_method(payload)
        except ClientError as e:
            if retry:
                logger.debug("Individual send attempt has failed, retrying")
                self._send_individual_payload(payload, retry-1)
            else:
                logger.error("Individual send attempt has failed, no more retries remaining")
                raise e

    def _handle_client_error(self, error, batch, send_as_individual_items=False):
        """ Handle a botocore.exceptions.ClientError """
        pass

    def _process_batch_send_response(self, response):
        """ Process the response data from a batch put request """
        pass

    def _batch_send_payloads(self, batch):
        """ Attempt to send a single batch of payloads to the subject """
        logger.debug("Sending batch of '{}' payloads to {}".format(len(batch), self.subject_name))
        try:
            if isinstance(batch, dict):
                response = self.batch_dispatch_method(**batch if isinstance(batch, dict) else batch)
                self._process_batch_send_response(response)
            else:
                response = self.batch_dispatch_method(batch)
                self._process_batch_send_response(response)
        except ClientError as e:
            self._handle_client_error(str(e), batch)

    def flush_payloads(self):
        """ Push all payloads in the payload list to the subject """
        logger.debug("Payload list has {} entries".format(len(self.payload_list)))
        if self.payload_list:
            logger.debug("Preparing to send {} records to {}".format(len(self.payload_list), self.subject_name))
            batch_list = list(chunks(self.payload_list, self.max_batch_size))
            for batch in batch_list:
                self._batch_send_payloads(batch)
            self.payload_list = []

    def _flush_payload_selector(self):
        """ Decide whether or not to flush the payload (usually used following a payload submission) """
        logger.debug("Payload list now contains '{}' payloads, max batch size is '{}'".format(
            len(self.payload_list), self.max_batch_size
        ))
        if self.flush_payload_on_max_batch_size and len(self.payload_list) == self.max_batch_size:
            logger.debug("Max batch size has been reached, flushing the payload list contents")
            self._batch_send_payloads(self.payload_list)

    def submit_payload(self, payload):
        """ Submit a metric ready to be batched up and sent to Cloudwatch """
        logger.debug("Payload has been submitted to the {} batch manager: {}".format(self.subject_name, payload))
        self.payload_list.append(payload)
        self._flush_payload_selector()

