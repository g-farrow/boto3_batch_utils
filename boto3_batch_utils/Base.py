import logging
import boto3
from botocore.exceptions import ClientError

from boto3_batch_utils.utils import chunks, get_byte_size_of_dict_or_list

logger = logging.getLogger('boto3-batch-utils')


_boto3_interface_type_mapper = {
    'dynamodb': 'resource',
    'kinesis': 'client',
    'sqs': 'client',
    'cloudwatch': 'client'
}


class BaseDispatcher:

    def __init__(self, aws_service: str, batch_dispatch_method: str, individual_dispatch_method: str = None,
                 max_batch_size: int = 1, flush_payload_on_max_batch_size: bool = True) -> None:
        """
        :param aws_service: object - the boto3 client which shall be called to dispatch each payload
        :param batch_dispatch_method: method - the method to be called when attempting to dispatch multiple items in a
        payload
        :param individual_dispatch_method: method - the method to be called when attempting to dispatch an individual
        item to the subject
        :param max_batch_size: int - Maximum size of a payload batch to be sent to the target
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
        self.max_batch_size = max_batch_size
        self.flush_payload_on_max_batch_size = flush_payload_on_max_batch_size
        self._aws_service_batch_max_payloads = None
        self._aws_service_message_max_bytes = None
        self._aws_service_batch_max_bytes = None
        self._batch_payload = None
        logger.debug(f"Batch dispatch initialised: {self.aws_service_name}")

    def _validate_initialisation(self):
        """
        Ensure that all the initialised values and attributes are valid
        """
        if self._aws_service_batch_max_payloads and self.max_batch_size > self._aws_service_batch_max_payloads:
            raise ValueError(f"Requested max_batch_size '{self.max_batch_size}' exceeds the {self.aws_service_name} "
                             f"maximum")

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
        logger.debug(f"{self.aws_service_name} payload list has {len(self._batch_payload)} entries")
        self._initialise_aws_client()
        if self._batch_payload:
            logger.debug(f"Preparing to send {len(self._batch_payload)} records to {self.aws_service_name}")
            batch_list = list(chunks(self._batch_payload, self.max_batch_size))
            logger.debug(f"Payload list split into {len(batch_list)} batches")
            for batch in batch_list:
                self._batch_send_payloads(batch)
            self._batch_payload = []
        else:
            logger.info(f"No payloads to flush to {self.aws_service_name}")

    def _flush_payload_selector(self):
        """ Decide whether or not to flush the payload (usually used following a payload submission) """
        logger.debug(f"Payload list now contains '{len(self._batch_payload)}' payloads, "
                     f"max batch size is '{self.max_batch_size}'")
        if self.flush_payload_on_max_batch_size and len(self._batch_payload) >= self.max_batch_size:
            logger.debug("Max batch size has been reached, flushing the payload list contents")
            self.flush_payloads()
        else:
            logger.debug(f"Max batch size of {self.max_batch_size} for {self.aws_service_name} "
                         "has not yet been reached, continuing")

    def _prevent_batch_bytes_overload(self, payload: dict):
        """ Check that adding appending the payload to the exiting batch does not overload the batch byte limit """
        current_batch_payload_byte_size = get_byte_size_of_dict_or_list(self._batch_payload)
        payload_byte_size = get_byte_size_of_dict_or_list(payload)
        if (current_batch_payload_byte_size + payload_byte_size) > self._aws_service_batch_max_bytes:
            logger.debug(f"Adding payload ({payload_byte_size} bytes) to the existing batch "
                         f"({current_batch_payload_byte_size} bytes) would exceed the batch limit for "
                         f"{self.aws_service_name}, calling flush_payloads")
            self.flush_payloads()

    def _validate_payload_byte_size(self, payload):
        """ Validate that the payload is within the byte size limit for the AWS service """
        if get_byte_size_of_dict_or_list(payload) > self._aws_service_message_max_bytes:
            raise ValueError(f"Submitted payload exceeds the maximum payload size for {self.aws_service_name}")

    def _append_payload_to_current_batch(self, payload):
        """ Append the payload to the service specific batch structure """
        pass

    def submit_payload(self, payload: dict):
        """ Submit a metric ready to be batched up and sent to Cloudwatch """
        self._validate_payload_byte_size(payload)
        self._prevent_batch_bytes_overload(payload)
        self._append_payload_to_current_batch(payload)
        logger.debug(f"Payload has been added to the {self.aws_service_name} dispatcher payload list: {payload}")
        self._flush_payload_selector()
