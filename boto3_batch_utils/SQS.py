import logging
from uuid import uuid4
from json import dumps, loads

from boto3_batch_utils.Base import BaseDispatcher
from boto3_batch_utils.utils import DecimalEncoder
from boto3_batch_utils import constants

logger = logging.getLogger('boto3-batch-utils')


class SQSBaseBatchDispatcher(BaseDispatcher):

    def __init__(self, queue_name, max_batch_size=10):
        self.queue_name = queue_name
        self.queue_url = None
        self.batch_in_progress = None
        self.fifo_queue = False
        super().__init__('sqs', batch_dispatch_method='send_message_batch', individual_dispatch_method='send_message',
                         max_batch_size=max_batch_size)
        self._aws_service_batch_max_payloads = constants.SQS_MAX_BATCH_PAYLOADS
        self._aws_service_message_max_bytes = constants.SQS_MESSAGE_MAX_BYTES
        self._aws_service_batch_max_bytes = constants.SQS_BATCH_MAX_BYTES
        self._batch_payload_wrapper = {'QueueUrl': self.queue_url, 'Entries': []}
        self._batch_payload = []
        self._validate_initialisation()

    def _batch_send_payloads(self, batch: (dict, list) = None, **kwargs):
        """ Attempt to send a single batch of records to SQS """
        if not self.queue_url:
            self.queue_url = self._aws_service.get_queue_url(QueueName=self.queue_name)['QueueUrl']
        self.batch_in_progress = batch
        if 'retry' in kwargs:
            super()._batch_send_payloads(batch, kwargs['retry'])
        else:
            super()._batch_send_payloads({'Entries': batch, 'QueueUrl': self.queue_url})

    def _process_batch_send_response(self, response: dict):
        """ Process the response data from a batch put request """
        logger.debug(f"Processing response: {response}")
        if "Failed" in response:
            logger.info(f"Failed payloads detected ({len(response['Failed'])}), processing errors...")
            for failed_payload_response in response['Failed']:
                logger.debug(f"Message failed with following error: {failed_payload_response['Message']}")
                if failed_payload_response['SenderFault']:
                    logger.warning(f"Message failed to send due to user error "
                                   f"({failed_payload_response['SenderFault']}): {failed_payload_response['Message']}")
                for payload in self.batch_in_progress:
                    if payload['Id'] == failed_payload_response['Id']:
                        self._send_individual_payload(payload)
        self.batch_in_progress = None

    def _unpack_failed_batch_to_unprocessed_items(self, batch: dict):
        """ Extract all records from the attempted batch payload """
        extracted_payloads = [self._unpack_individual_failed_payload(pl) for pl in batch['Entries']]
        self.unprocessed_items = self.unprocessed_items + extracted_payloads

    def _unpack_individual_failed_payload(self, payload: dict, retry: int = 4):
        """ Send an individual payload to Kinesis """
        return loads(payload['MessageBody'])


class SQSBatchDispatcher(SQSBaseBatchDispatcher):
    """
    Manage the batch 'send' of SQS messages
    """

    def __init__(self, queue_name, max_batch_size=10):
        super().__init__(queue_name, max_batch_size)
        self.fifo_queue = False

    def __str__(self):
        return f"SQSBatchDispatcher::{self.queue_name}"

    def submit_payload(self, payload: dict, message_id="", delay_seconds=None, message_group_id: str = 'unset'):
        """ Submit a record ready to be batched up and sent to SQS """
        logger.debug(f"Payload submitted to SQS dispatcher: {payload}")
        message_id = message_id or str(uuid4())
        if not any(d["Id"] == message_id for d in self._batch_payload):
            constructed_payload = {
                'Id': message_id,
                'MessageBody': dumps(payload, cls=DecimalEncoder)
                }
            if isinstance(delay_seconds, int):
                constructed_payload['DelaySeconds'] = delay_seconds
            logger.debug(f"SQS payload constructed: {constructed_payload}")
            super().submit_payload(constructed_payload)
        else:
            logger.debug(f"Message with message_id ({message_id}) already exists in the batch, skipping...")

    def _send_individual_payload(self, payload: dict, retry: int = 4):
        """ Send an individual record to SQS """
        kwargs = {'QueueUrl': self.queue_url, 'MessageBody': payload['MessageBody']}
        if payload.get('DelaySeconds'):
            kwargs['DelaySeconds'] = payload['DelaySeconds']
        super()._send_individual_payload(kwargs, retry)


class SQSFifoBatchDispatcher(SQSBaseBatchDispatcher):

    def __init__(self, queue_name, max_batch_size=10, content_based_deduplication=False):
        super().__init__(queue_name, max_batch_size)
        self.fifo_queue = True
        self.content_based_deduplication = content_based_deduplication

    def __str__(self):
        return f"SQSFifoBatchDispatcher::{self.queue_name}"

    def submit_payload(self, payload: dict, message_id=str(uuid4()), delay_seconds: int = None,
                       message_group_id: str = 'unset', message_deduplication_id: str = None):
        """ Submit a record ready to be batched up and sent to SQS """
        logger.debug(f"Payload submitted to SQS FIFO dispatcher: {payload}")
        constructed_payload = {
            'Id': message_id,
            'MessageBody': dumps(payload, cls=DecimalEncoder),
            'MessageGroupId': message_group_id
        }
        if message_deduplication_id:
            if not any(d['MessageDeduplicationId'] == message_deduplication_id for d in self._batch_payload):
                constructed_payload['MessageDeduplicationId'] = message_deduplication_id
            else:
                logger.debug(f"Message with message_id ({message_id}) already exists in the batch, skipping...")
                return
        elif not self.content_based_deduplication:
            raise ValueError(f"Target SQS FIFO queue ({self.queue_name}) is not shown to have ContentBasedDeduplication"
                             f" therefore `message_deduplication_id` MUST be set")
        logger.debug(f"SQS FIFO payload constructed: {constructed_payload}")
        super().submit_payload(constructed_payload)

    def _send_individual_payload(self, payload: dict, retry: int = 4):
        """ Send an individual record to SQS """
        kwargs = {
            'QueueUrl': self.queue_url,
            **payload
        }
        super()._send_individual_payload(kwargs, retry)
