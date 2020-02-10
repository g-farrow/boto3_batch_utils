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

    def __init__(self, aws_service: str, batch_dispatch_method: str, individual_dispatch_method: str = None,
                 batch_size: int = 1, flush_payload_on_max_batch_size: bool = True) -> None:
        """
        :param aws_service: object - the boto3 client which shall be called to dispatch each payload
        :param batch_dispatch_method: method - the method to be called when attempting to dispatch multiple items in a
        payload
        :param individual_dispatch_method: method - the method to be called when attempting to dispatch an individual
        item to the subject
        :param batch_size: int - Maximum size of a payload batch to be sent to the target
        :param flush_payload_on_max_batch_size: bool - should payload be automatically sent once the payload size is
        equal to that of the maximum permissible batch (True), or should the manager wait for a flush payload call
        (False)
        """
        self.aws_service_name = aws_service
        self._aws_service = None
        self.batch_dispatch_method = batch_dispatch_method
        self._batch_dispatch_method = None
        self.individual_dispatch_method = individual_dispatch_method
        self._individual_dispatch_method = None
        self.max_batch_size = batch_size
        self.flush_payload_on_max_batch_size = flush_payload_on_max_batch_size
        self._payload_list = []
        logger.debug(f"Batch dispatch initialised: {self.aws_service_name}")

    def _initialise_aws_client(self):
        """
        Initialise client/resource for the AWS service
        """
        if not self._aws_service:
            self._aws_service = getattr(boto3, _boto3_interface_type_mapper[self.aws_service_name])(
                self.aws_service_name)
            self._batch_dispatch_method = getattr(self._aws_service, str(self.batch_dispatch_method))
            if self.individual_dispatch_method:
                self._individual_dispatch_method = getattr(self._aws_service, self.individual_dispatch_method)
            else:
                self._individual_dispatch_method = None
        logger.debug("AWS/Boto3 Client is now initialised")

    def _send_individual_payload(self, payload: (dict, str), retry: int = 4):
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
                raise e

    def _process_batch_send_response(self, response):
        """ Process the response data from a batch put request """
        pass

    def _batch_send_payloads(self, batch: (list, dict), retry: int = 4):
        """ Attempt to send a single batch of payloads to the subject """
        logger.debug(f"Sending batch type {type(batch)} payloads to {self.aws_service_name}")
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
                logger.warning(f"{self.aws_service_name} batch send has caused an error, "
                               f"retrying to send ({retry} retries remaining): {str(e)}")
                logger.debug(f"Failed batch: (type: {type(batch)}) {batch}")
                self._batch_send_payloads(batch, retry=retry-1)
            else:
                raise

    def flush_payloads(self):
        """ Push all payloads in the payload list to the subject """
        logger.debug(f"{self.aws_service_name} payload list has {len(self._payload_list)} entries")
        self._initialise_aws_client()
        if self._payload_list:
            logger.debug(f"Preparing to send {len(self._payload_list)} records to {self.aws_service_name}")
            batch_list = list(chunks(self._payload_list, self.max_batch_size))
            logger.debug(f"Payload list split into {len(batch_list)} batches")
            for batch in batch_list:
                self._batch_send_payloads(batch)
            self._payload_list = []
        else:
            logger.info(f"No payloads to flush to {self.aws_service_name}")

    def _flush_payload_selector(self):
        """ Decide whether or not to flush the payload (usually used following a payload submission) """
        logger.debug(f"Payload list now contains '{len(self._payload_list)}' payloads, "
                     f"max batch size is '{self.max_batch_size}'")
        if self.flush_payload_on_max_batch_size and len(self._payload_list) >= self.max_batch_size:
            logger.debug("Max batch size has been reached, flushing the payload list contents")
            self.flush_payloads()
        else:
            logger.debug(f"Max batch size of {self.max_batch_size} for {self.aws_service_name} "
                         "has not yet been reached, continuing")

    def submit_payload(self, payload):
        """ Submit a metric ready to be batched up and sent to Cloudwatch """
        self._payload_list.append(payload)
        logger.debug(f"Payload has been added to the {self.aws_service_name} dispatcher payload list: {payload}")
        self._flush_payload_selector()
