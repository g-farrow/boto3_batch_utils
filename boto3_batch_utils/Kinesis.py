import logging
from json import dumps

from boto3_batch_utils.Base import BaseDispatcher
from boto3_batch_utils.utils import DecimalEncoder
from boto3_batch_utils.constants import KINESIS_BATCH_MAX_BYTES, KINESIS_BATCH_MAX_PAYLOADS, KINESIS_MESSAGE_MAX_BYTES

logger = logging.getLogger('boto3-batch-utils')

kinesis_max_batch_size = 250


class KinesisBatchDispatcher(BaseDispatcher):
    """
    Manage the batch 'put' of Kinesis records
    """

    def __init__(self, stream_name: str, partition_key_identifier: str = 'Id', max_batch_size: int = 250,
                 flush_payload_on_max_batch_size: bool = True):
        self.stream_name = stream_name
        self.partition_key_identifier = partition_key_identifier
        self.batch_in_progress = []
        super().__init__('kinesis', batch_dispatch_method='put_records', individual_dispatch_method='put_record',
                         max_batch_size=max_batch_size, flush_payload_on_max_batch_size=flush_payload_on_max_batch_size)
        self._aws_service_batch_max_payloads = KINESIS_BATCH_MAX_PAYLOADS
        self._aws_service_message_max_bytes = KINESIS_MESSAGE_MAX_BYTES
        self._aws_service_batch_max_bytes = KINESIS_BATCH_MAX_BYTES
        self._batch_payload = {'StreamName': self.stream_name, 'Records': []}
        self._validate_initialisation()

    def __str__(self):
        return f"KinesisBatchDispatcher::{self.stream_name}"

    def _send_individual_payload(self, payload: dict, retry: int = 5):
        """ Send an individual payload to Kinesis """
        _payload = payload
        _payload['StreamName'] = self.stream_name
        super()._send_individual_payload(_payload)

    def _process_failed_payloads(self, response: dict):
        """ Process the contents of a Put Records response when it contains failed records """
        i = 0
        failed_records = []
        for r in response["Records"]:
            logger.debug(f"Response: {r}")
            if "ErrorCode" in r:
                logger.warning(f"Payload failed to be sent to Kinesis. Message content: {r}")
                failed_records.append(i)
            i += 1
        successful_message_count = len(self.batch_in_progress) - len(failed_records)
        if successful_message_count:
            logger.info(f"Sent messages to kinesis {successful_message_count}")
        if failed_records:
            logger.debug(f"Failed Records: {response['FailedRecordCount']}")
            batch_of_problematic_records = [self.batch_in_progress[i] for i in failed_records]
            if len(failed_records) <= 2:
                for payload in batch_of_problematic_records:
                    self._send_individual_payload(payload)
            else:
                self._batch_send_payloads(batch_of_problematic_records)
        self.batch_in_progress = None

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

    def _batch_send_payloads(self, batch: (list, dict) = None, **kwargs):
        """ Attempt to send a single batch of metrics to Kinesis """
        if 'retry' in kwargs:
            self.batch_in_progress = batch['Records']
            super()._batch_send_payloads(batch, kwargs['retry'])
        else:
            self.batch_in_progress = batch
            super()._batch_send_payloads({'StreamName': self.stream_name, 'Records': batch})

    def flush_payloads(self):
        """ Push all metrics in the payload list to Kinesis """
        super().flush_payloads()

    def _append_payload_to_current_batch(self, payload):
        """ Append the payload to the service specific batch structure """
        self._batch_payload['Records'].append(payload)

    def submit_payload(self, payload: dict):
        """ Submit a metric ready to be batched up and sent to Kinesis """
        logger.debug(f"Payload submitted to {self.aws_service_name} dispatcher: {payload}")
        constructed_payload = {
            'Data': dumps(payload, cls=DecimalEncoder),
            'PartitionKey': f'{payload[self.partition_key_identifier]}'
        }
        super().submit_payload(constructed_payload)
