import logging
import boto3
from botocore.exceptions import ClientError

from boto3_batch_utils.utils import chunks


logger = logging.getLogger()

dynamodb_batch_get_limit = 100
dynamodb_batch_write_limit = 25
dynamodb = boto3.resource('dynamodb')


class DynamoWriteManager:
    """
    Control the submission of writes to DynamoDB
    """

    def __init__(self, dynamo_table_name, primary_partition_key):
        self.max_batch_size = dynamodb_batch_write_limit
        self.records_to_write = []
        self.target_table_name = dynamo_table_name
        self.primary_partition_key = primary_partition_key
        self.target_entity_table = dynamodb.Table(dynamo_table_name)

    def _send_individual_record(self, record):
        """
        Write an individual record to Dynamo
        :param record: JSON representation of a new record to write
        """
        logger.debug("Preparing to write individual record: {}".format(record))
        record_content = record['PutRequest']['Item']
        record_content['Id'] = record_content[self.primary_partition_key]
        logger.info("Writing record: {}".format(record_content))
        try:
            self.target_entity_table.put_item(Item=record_content)
        except ClientError as e:
            logger.error("Individual write attempt resulted in an exception: {}".format(e))

    def _process_batch_write_response(self, response):
        """
        Parse the response from a batch_write call, handle any failures as required.
        :param response: Response JSON from a batch_write_item request
        """
        unprocessed_items = response['UnprocessedItems']
        if unprocessed_items:
            logger.warning("Batch write failed to write all items, {} were rejected".format(
                len(unprocessed_items[self.target_table_name])))
            for item in unprocessed_items[self.target_table_name]:
                self._send_individual_record(item)

    def _write_batch_to_dynamo(self, batch):
        """
        Submit the batch to DynamoDB
        """
        logger.debug("Attempting to submit {} record to Dynamo".format(len(batch)))
        payload = {self.target_table_name: batch}
        logger.debug("Batch write payload: {}".format(payload))
        try:
            response = dynamodb.batch_write_item(RequestItems=payload)
            logger.debug("Post DynamoDB response: {}".format(response))
            self._process_batch_write_response(response)
        except ClientError as e:
            logger.warning("Batch Write Error: {}".format(e))
            logger.info("Processing writes individually")
            for record in batch:
                self._send_individual_record(record)
        logger.info("Batch completed")

    def submit_record(self, record):
        """
        Submit a metric ready for batch sending to Cloudwatch
        """
        record_payload = record
        logger.debug(record_payload)
        logger.info(
            "Target record has key {}:{} - converting it to string as required"
            "".format(self.primary_partition_key, record_payload[self.primary_partition_key]))
        record_payload[self.primary_partition_key] = str(record_payload[self.primary_partition_key])
        logger.debug("Appending following record to the queue: {}".format(record_payload))
        if record not in self.records_to_write:
            self.records_to_write.append(
                {"PutRequest": {"Item": record_payload}}
            )
            logger.debug("Dynamo batch size is now {}".format(len(self.records_to_write)))
        else:
            logger.info("Record is a duplicate of an existing record in the batch write queue, skipping")

    def write_records(self):
        """
        Send any metrics remaining in the current batch bucket
        """
        logger.debug("Records to write list has {} entries".format(len(self.records_to_write)))
        if self.records_to_write:
            logger.debug("Preparing to write {} records to Dynamo".format(len(self.records_to_write)))
            batch_list = list(chunks(self.records_to_write, self.max_batch_size))
            for batch in batch_list:
                self._write_batch_to_dynamo(batch)
