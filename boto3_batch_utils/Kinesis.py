import logging
from json import dumps

from boto3_batch_utils.Base import BaseDispatcher
from boto3_batch_utils.utils import DecimalEncoder

logger = logging.getLogger('boto3-batch-utils')


kinesis_max_batch_size = 250


class KinesisBatchDispatcher(BaseDispatcher):
    """
    Manage the batch 'put' of Kinesis records
    """

    def __init__(self, stream_name, partition_key_identifier='Id', max_batch_size=250,
                 flush_payload_on_max_batch_size=True):
        self.stream_name = stream_name
        self.partition_key_identifier = partition_key_identifier
        self.batch_in_progress = []
        super().__init__('kinesis', 'put_records', 'put_record', max_batch_size, flush_payload_on_max_batch_size)

    def _send_individual_payload(self, payload, retry=5):
        """ Send an individual payload to Kinesis """
        _payload = payload
        _payload['StreamName'] = self.stream_name
        super()._send_individual_payload(_payload)

    def _process_failed_payloads(self, response):
        """ Process the contents of a Put Records response when it contains failed records """
        i = 0
        failed_records = []
        for r in response["Records"]:
            logger.debug("Response: {}".format(r))
            if "ErrorCode" in r:
                logger.warning("Payload failed to be sent to Kinesis. Message content: {}".format(r))
                failed_records.append(i)
            i += 1
        successful_message_count = len(self.batch_in_progress) - len(failed_records)
        if successful_message_count:
            logger.info("Sent messages to kinesis {}".format(successful_message_count))
        if failed_records:
            logger.debug(
                "Failed Records: {}".format(response["FailedRecordCount"]))
            batch_of_problematic_records = [self.batch_in_progress[i] for i in failed_records]
            if len(failed_records) <= 2:
                for payload in batch_of_problematic_records:
                    self._send_individual_payload(payload)
            else:
                self._batch_send_payloads(batch_of_problematic_records)
        self.batch_in_progress = None

    def _process_batch_send_response(self, response):
        """
        Method to send a set of messages on to the Kinesis stream
        :param batch: List - messages to be sent
        :param nested: bool - Used for recursion identification. Do not override.
        """
        logger.debug("Processing response: {}".format(response))
        if "Records" in response:
            if response["FailedRecordCount"] == 0:
                logger.info("{} records successfully batch sent to Kinesis::{}".format(len(self.batch_in_progress),
                                                                                       self.stream_name))
                self.batch_in_progress = None
                return
            else:
                logger.info("Failed payloads detected ({}), processing errors...".format(response["FailedRecordCount"]))
                self._process_failed_payloads(response)

    def _batch_send_payloads(self, batch=None, **kwargs):
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

    def submit_payload(self, payload):
        """ Submit a metric ready to be batched up and sent to Kinesis """
        logger.debug("Payload submitted to {} dispatcher: {}".format(self._subject_name, payload))
        constructed_payload = {
            'Data': dumps(payload, cls=DecimalEncoder),
            'PartitionKey': '{}'.format(payload[self.partition_key_identifier])
        }
        super().submit_payload(constructed_payload)
