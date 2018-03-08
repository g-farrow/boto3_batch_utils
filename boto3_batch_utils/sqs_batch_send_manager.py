from uuid import uuid4
from boto3_batch_utils.base_dispatcher import BaseBatchManager


sqs_batch_send_batch_size = 10


class SQSBatchSendManager(BaseBatchManager):
    """
    Manage the batch 'send' of SQS messages
    """

    def __init__(self, queue_name, max_batch_size=sqs_batch_send_batch_size, flush_payload_on_max_batch_size=True):
        self.queue_name = queue_name
        super().__init__('sqs', 'send_messages', 'send_message', max_batch_size, flush_payload_on_max_batch_size)
        self.queue_url = self.subject.get_queue_url(QueueName=self.queue_name)['QueueUrl']

    def _send_individual_payload(self, metric, retry=5):
        """ Send an individual metric to Cloudwatch """
        super()._send_individual_payload(metric)

    def _process_batch_send_response(self, response):
        """ Process the response data from a batch put request """
        pass

    def _batch_send_payloads(self, batch=None, **nested_batch):
        """ Attempt to send a single batch of metrics to Cloudwatch """
        super()._batch_send_payloads(**{'QueueUrl': self.queue_url, 'Entries': batch})

    def flush_payloads(self):
        """ Push all metrics in the payload list to Cloudwatch """
        super().flush_payloads()

    def submit_payload(self, payload, message_id=None, delay_seconds=0):
        """ Submit a metric ready to be batched up and sent to Cloudwatch """
        constructed_payload = {
            'Id': message_id if message_id else str(uuid4()),
            'MessageBody': payload,
            'DelaySeconds': delay_seconds
            }
        super().submit_payload(constructed_payload)
