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
        super().__init__('sqs', 'send_message_batch', 'send_message', max_batch_size, flush_payload_on_max_batch_size)
        self.queue_url = None
        self.batch_in_progress = None

    def _send_individual_payload(self, payload, retry=5):
        """ Send an individual record to SQS """
        super()._send_individual_payload(
            {'QueueUrl': self.queue_url, 'MessageBody': payload['MessageBody'],
             'DelaySeconds': payload['DelaySeconds']},
            retry=4
        )

    def _process_batch_send_response(self, response):
        """ Process the response data from a batch put request """
        logger.debug("Processing response: {}".format(response))
        if "Failed" in response:
            logger.info("Failed payloads detected ({}), processing errors...".format(len(response["Failed"])))
            for failed_payload_response in response['Failed']:
                logger.debug("Message failed with following error: {}".format(failed_payload_response['Message']))
                if failed_payload_response['SenderFault']:
                    logger.warning("Message failed to send due to user error ({}): {}".format(
                        failed_payload_response['SenderFault'], failed_payload_response['Message']))
                for payload in self.batch_in_progress:
                    if payload['Id'] == failed_payload_response['Id']:
                        self._send_individual_payload(payload)
        self.batch_in_progress = None

    def _batch_send_payloads(self, batch=None, **nested_batch):
        """ Attempt to send a single batch of records to SQS """
        if not self.queue_url:
            self.queue_url = self._subject.get_queue_url(QueueName=self.queue_name)['QueueUrl']
        self.batch_in_progress = batch
        super()._batch_send_payloads({'QueueUrl': self.queue_url, 'Entries': batch})

    def flush_payloads(self):
        """ Push all records in the payload list to SQS """
        super().flush_payloads()

    def submit_payload(self, payload, message_id=str(uuid4()), delay_seconds=0):
        """ Submit a record ready to be batched up and sent to SQS """
        logger.debug("Payload submitted to {} dispatcher: {}".format(self._subject_name, payload))
        if not any(d["Id"] == message_id for d in self._payload_list):
            constructed_payload = {
                'Id': message_id,
                'MessageBody': str(payload),
                'DelaySeconds': delay_seconds
                }
            logger.debug("SQS payload constructed: {}".format(constructed_payload))
            super().submit_payload(constructed_payload)
        else:
            logger.debug("Message with the provided message_id ({}) already exists in the batch, skipping...".format(
                message_id))
