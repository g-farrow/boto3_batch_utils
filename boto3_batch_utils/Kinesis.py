import logging
from json import dumps, loads
from copy import deepcopy
from uuid import uuid4

from boto3_batch_utils.Base import BaseDispatcher
from boto3_batch_utils.utils import DecimalEncoder
from boto3_batch_utils import constants


logger = logging.getLogger('boto3-batch-utils')


class KinesisBatchDispatcher(BaseDispatcher):
    """
    Manage the batch 'put' of Kinesis records
    """

    def __init__(self, stream_name: str, partition_key_identifier: str = None, max_batch_size: int = 250):
        self.stream_name = stream_name
        self.partition_key_identifier = partition_key_identifier
        self.batch_in_progress = []
        super().__init__('kinesis', batch_dispatch_method='put_records', individual_dispatch_method='put_record',
                         max_batch_size=max_batch_size)
        self._aws_service_batch_max_payloads = constants.KINESIS_BATCH_MAX_PAYLOADS
        self._aws_service_message_max_bytes = constants.KINESIS_MESSAGE_MAX_BYTES
        self._aws_service_batch_max_bytes = constants.KINESIS_BATCH_MAX_BYTES
        self._batch_payload_wrapper = {'StreamName': self.stream_name, 'Records': []}
        self._batch_payload = []
        self._validate_initialisation()

    def __str__(self):
        return f"KinesisBatchDispatcher::{self.stream_name}"

    def submit_payload(self, payload: dict):
        """ Submit a metric ready to be batched up and sent to Kinesis """
        logger.debug(f"Payload submitted to {self.aws_service_name} dispatcher: {payload}")
        constructed_payload = {
            'Data': dumps(payload, cls=DecimalEncoder),
            'PartitionKey': f'{payload[self.partition_key_identifier] if self.partition_key_identifier else uuid4()}'
        }
        super().submit_payload(constructed_payload)

    def _batch_send_payloads(self, batch: (list, dict) = None, **kwargs):
        """ Attempt to send a single batch of metrics to Kinesis """
        self.batch_in_progress = batch
        if isinstance(batch, list):
            batch = {'StreamName': self.stream_name, 'Records': batch}
        if 'retry' in kwargs:
            super()._batch_send_payloads(batch, kwargs['retry'])
        else:
            super()._batch_send_payloads(batch)

    def _process_batch_send_response(self, response: dict):
        """
        Method to send a set of messages on to the Kinesis stream
        :param response: Response from the AWS service
        """
        logger.debug(f"Processing response: {response}")
        if "Records" in response:
            if response["FailedRecordCount"] == 0:
                logger.info(f"{len(self.batch_in_progress)} records successfully batch "
                            f"sent to Kinesis::{self.stream_name}")
                self.batch_in_progress = None
                return
            else:
                logger.info(f"Failed payloads detected ({response['FailedRecordCount']}), processing errors...")
                self._process_failed_payloads(response)

    def _process_failed_payloads(self, response: dict, retry=3):
        """ Process the contents of a Put Records response when it contains failed records """
        failed_records = self._get_index_of_failed_record(response)
        if failed_records:
            logger.debug(f"Failed Records: {response['FailedRecordCount']}")
            batch_of_problematic_records = []
            for r in failed_records:
                batch_of_problematic_records.append(self.batch_in_progress[r])
            if len(failed_records) <= 2:
                for payload in batch_of_problematic_records:
                    self._send_individual_payload(deepcopy(payload))
            else:
                self._batch_send_payloads(batch_of_problematic_records, retry=retry)
        self.batch_in_progress = None

    @staticmethod
    def _get_index_of_failed_record(response: dict) -> list:
        """ Parse the response object and identify which records failed and return an array of their index positions
         within the batch """
        i = 0
        failed_records = []
        for r in response["Records"]:
            logger.debug(f"Response: {r}")
            if "ErrorCode" in r:
                logger.warning(f"Payload failed to be sent to Kinesis. Message content: {r}")
                failed_records.append(i)
            i += 1
        return failed_records

    def _unpack_failed_batch_to_unprocessed_items(self, batch: dict):
        """ Extract all records from the attempted batch payload """
        extracted_payloads = [self._unpack_individual_failed_payload(pl) for pl in batch['Records']]
        self.unprocessed_items = self.unprocessed_items + extracted_payloads

    def _send_individual_payload(self, payload: dict, retry: int = 4):
        """ Send an individual payload to Kinesis """
        _payload = payload
        _payload['StreamName'] = self.stream_name
        super()._send_individual_payload(_payload, retry)

    def _unpack_individual_failed_payload(self, payload):
        """ Extract the record from a constructed payload """
        return loads(payload['Data'])
