import logging
import boto3
from decimal import Decimal
from json import dumps, JSONEncoder

from boto3_batch_utils.utils import chunks

logger = logging.getLogger()


class DecimalEncoder(JSONEncoder):
    """
    Helper class to convert a DynamoDB item to JSON.
    """
    def default(self, o):
        if isinstance(o, Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class KinesisManager:
    """
    Control the submission of puts to a Kinesis queue
    """

    def __init__(self, stream_name, partition_key='Id', max_batch_size=250):
        """
        :param stream_name: The name of the Kinesis stream which all messges will be posted to
        :param partition_key: The key name within each message, whose value will be used as the partition key for the
        record
        :param max_batch_size: The maximum number of messages to be sent to kinesis in any, one, put_records request
        """
        self.max_batch_size = max_batch_size
        self.partition_key = partition_key
        self.records_to_send = []
        self.kinesis_client = boto3.client('kinesis')
        self.stream_name = stream_name

    def submit_record(self, record):
        """
        Submit a metric ready for batch sending to Cloudwatch
        """
        logger.debug("Appending following record to the queue: {}".format(record))
        self.records_to_send.append(
            {'Data': dumps(record, cls=DecimalEncoder), 'PartitionKey': '{}'.format(record['record_key'][self.partition_key])}
        )

    def _send_single_batch_to_kinesis(self, batch, nested=False):
        """
        Method to send a set of messages on to the Kinesis stream
        :param batch: List - messages to be sent
        :param nested: bool - Used for recursion identification. Do not override.
        """
        logger.debug("Attempting to send {} records to Kinesis::{}".format(len(batch), self.stream_name))
        response = self.kinesis_client.put_records(StreamName=self.stream_name, Records=batch)
        if "Records" in response:
            i = 0
            failed_records = []
            for r in response["Records"]:
                logger.debug("Response: {}".format(r))
                if "ErrorCode" in r:
                    logger.debug(
                        "Message failed to be processed, message will be retried. Message content: {}".format(r))
                    failed_records.append(i)
                i += 1
            successful_message_count = len(batch) - len(failed_records)
            if successful_message_count:
                logger.info("Sent messages to kinesis {}".format(successful_message_count))
            if failed_records:
                logger.debug(
                    "Failed Records: {}, Problems: {}".format(response["FailedRecordCount"], len(failed_records)))
                batch_of_problematic_records = [batch[i] for i in failed_records]
                self._send_single_batch_to_kinesis(batch_of_problematic_records, True)
            elif nested:
                logger.debug("Partial batch of {} records completed without error".format(len(batch)))

    def _split_records_to_send_into_batches(self):
        """
        Split the records to send into batches of less than or equal size to the maximum
        """
        batches = list(chunks(self.records_to_send, self.max_batch_size))
        logger.info("{} records have been split into {} batches of {} or less".format(
            len(self.records_to_send), len(batches), self.max_batch_size)
        )
        return batches

    def flush_all_records_to_kinesis(self):
        logger.info("Preparing to send {} messages to Kinesis::{}".format(len(self.records_to_send), self.stream_name))
        batches = self._split_records_to_send_into_batches()
        for batch in batches:
            self._send_single_batch_to_kinesis(batch)
        self.records_to_send = []
        logger.info(
            "Messages flushed to Kinesis::{} - records to send list has been reset to empty".format(self.stream_name))
