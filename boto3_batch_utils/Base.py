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
        logger.debug(f"Batch dispatch manager initialised: {self._subject_name}")

    def _send_individual_payload(self, payload, retry=4):
        """ Send an individual payload to the subject """
        logger.debug(f"Attempting to send individual payload ({retry} retries left): {payload}")
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
        logger.debug("Sending batch type {type(batch)} payloads to {self._subject_name}")
        try:
            if isinstance(batch, dict):
                response = self._batch_dispatch_method(**batch)
                self._process_batch_send_response(response)
            else:
                response = self._batch_dispatch_method(batch)
                self._process_batch_send_response(response)
            logger.debug(f"Batch send response: {response}")
        except ClientError as e:
            if retry > 0:
                logger.warning(f"{self._subject_name} batch send has caused an error, "
                               f"retrying to send ({retry} retries remaining): {str(e)}")
                logger.debug(f"Failed batch: (type: {type(batch)}) {batch}")
                self._batch_send_payloads(batch, retry=retry-1)
            else:
                raise

    def flush_payloads(self):
        """ Push all payloads in the payload list to the subject """
        logger.debug(f"{self._subject_name} payload list has {len(self._payload_list)} entries")
        if self._payload_list:
            logger.debug(f"Preparing to send {len(self._payload_list)} records to {self._subject_name}")
            batch_list = list(chunks(self._payload_list, self.max_batch_size))
            logger.debug("Payload list split into {len(batch_list)} batches")
            for batch in batch_list:
                self._batch_send_payloads(batch)
            self._payload_list = []
        else:
            logger.info(f"No payloads to flush to {self._subject_name}")

    def _flush_payload_selector(self):
        """ Decide whether or not to flush the payload (usually used following a payload submission) """
        logger.debug(f"Payload list now contains '{len(self._payload_list)}' payloads, "
                     f"max batch size is '{self.max_batch_size}'")
        if self.flush_payload_on_max_batch_size and len(self._payload_list) >= self.max_batch_size:
            logger.debug("Max batch size has been reached, flushing the payload list contents")
            self.flush_payloads()
        else:
            logger.debug("Max batch size of {self.max_batch_size} for {self._subject_name} "
                         "has not yet been reached, continuing")

    def submit_payload(self, payload):
        """ Submit a metric ready to be batched up and sent to Cloudwatch """
        self._payload_list.append(payload)
        logger.debug("Payload has been added to the {self._subject_name} dispatcher payload list: {payload}")
        self._flush_payload_selector()

