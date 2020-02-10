import logging
from uuid import uuid4
from boto3_batch_utils.Base import BaseDispatcher

logger = logging.getLogger('boto3-batch-utils')


class SQSBatchDispatcher(BaseDispatcher):
    """
    Manage the batch 'send' of SQS messages
    """

    def __init__(self, queue_name, max_batch_size=10, flush_payload_on_max_batch_size=True):
        self.queue_name = queue_name
        super().__init__('sqs', batch_dispatch_method='send_message_batch', individual_dispatch_method='send_message',
                         batch_size=max_batch_size, flush_payload_on_max_batch_size=flush_payload_on_max_batch_size)
        self.queue_url = None
        self.batch_in_progress = None
        self.fifo_queue = False

    def _send_individual_payload(self, payload: dict, retry: int = 5):
        """ Send an individual record to SQS """
        kwargs = {'QueueUrl': self.queue_url, 'MessageBody': payload['MessageBody']}
        if not self.fifo_queue and payload.get('DelaySeconds'):
            kwargs['DelaySeconds'] = payload['DelaySeconds']
        super()._send_individual_payload(
            kwargs,
            retry=4
        )

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

    def _batch_send_payloads(self, batch: (dict, list) = None, **nested_batch):
        """ Attempt to send a single batch of records to SQS """
        if not self.queue_url:
            self.queue_url = self._aws_service.get_queue_url(QueueName=self.queue_name)['QueueUrl']
        self.batch_in_progress = batch
        super()._batch_send_payloads({'QueueUrl': self.queue_url, 'Entries': batch})

    def flush_payloads(self):
        """ Push all records in the payload list to SQS """
        super().flush_payloads()

    def submit_payload(self, payload: dict, message_id=str(uuid4()), delay_seconds=None):
        """ Submit a record ready to be batched up and sent to SQS """
        logger.debug(f"Payload submitted to {self.aws_service_name} dispatcher: {payload}")
        if not any(d["Id"] == message_id for d in self._payload_list):
            constructed_payload = {
                'Id': message_id,
                'MessageBody': str(payload)
                }
            if not self.fifo_queue and isinstance(delay_seconds, int):
                constructed_payload['DelaySeconds'] = delay_seconds
            logger.debug(f"SQS payload constructed: {constructed_payload}")
            super().submit_payload(constructed_payload)
        else:
            logger.debug(f"Message with message_id ({message_id}) already exists in the batch, skipping...")


class SQSFifoBatchDispatcher(SQSBatchDispatcher):

    def __init__(self, queue_name, max_batch_size=10, flush_payload_on_max_batch_size=True):
        super().__init__(queue_name, max_batch_size, flush_payload_on_max_batch_size)
        self.fifo_queue = True
