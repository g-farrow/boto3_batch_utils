import logging
from boto3.dynamodb.types import TypeSerializer

from boto3_batch_utils.Base import BaseDispatcher
from boto3_batch_utils.utils import convert_floats_in_dict_to_decimals

logger = logging.getLogger('boto3-batch-utils')


class DynamoBatchDispatcher(BaseDispatcher):
    """
    Control the submission of writes to DynamoDB
    """

    def __init__(self, dynamo_table_name, primary_partition_key, partition_key_data_type=str,
                 max_batch_size=25, flush_payload_on_max_batch_size=True):
        self.dynamo_table_name = dynamo_table_name
        self.primary_partition_key = primary_partition_key
        self.partition_key_data_type = partition_key_data_type
        super().__init__('dynamodb', 'batch_write_item', 'put_item', batch_size=max_batch_size,
                         flush_payload_on_max_batch_size=flush_payload_on_max_batch_size)

    def _send_individual_payload(self, payload, retry=4):
        """
        Write an individual record to Dynamo
        :param payload: JSON representation of a new record to write to the Dynamo table
        """
        super()._send_individual_payload(payload, retry=4)

    def _process_batch_send_response(self, response):
        """
        Parse the response from a batch_write call, handle any failures as required.
        :param response: Response JSON from a batch_write_item request
        """
        unprocessed_items = response['UnprocessedItems']
        if unprocessed_items:
            logger.warning("Batch write failed to write all items, {} were rejected".format(
                len(unprocessed_items[self.dynamo_table_name])))
            for item in unprocessed_items[self.dynamo_table_name]:
                self._send_individual_payload(item)

    def _batch_send_payloads(self, batch=None, **nested_batch):
        """
        Submit the batch to DynamoDB
        """
        super()._batch_send_payloads({'RequestItems': {self.dynamo_table_name: batch}})

    def flush_payloads(self):
        """
        Send any metrics remaining in the current batch bucket
        """
        super().flush_payloads()

    def submit_payload(self, payload, partition_key_location="Id"):
        """
        Submit a metric ready for batch sending to Cloudwatch
        """
        logger.debug("Payload submitted to {} dispatcher: {}".format(self._subject_name, payload))
        if self.primary_partition_key not in payload.keys():
            payload[self.primary_partition_key] = self.partition_key_data_type(payload[partition_key_location])
        if not any(d["PutRequest"]["Item"][self.primary_partition_key] == payload[self.primary_partition_key] for d in self._payload_list):
            super().submit_payload(
                {"PutRequest": {
                    "Item": TypeSerializer().serialize(convert_floats_in_dict_to_decimals(payload))
                }}
            )
        else:
            logger.warning("The candidate payload has a primary_partition_key which already exists in the "
                           "payload_list: {}".format(payload))
