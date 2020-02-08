import logging
from botocore.exceptions import ClientError

from boto3_batch_utils.Base import BaseDispatcher
from boto3_batch_utils.utils import convert_floats_in_dict_to_decimals

logger = logging.getLogger('boto3-batch-utils')


class DynamoBatchDispatcher(BaseDispatcher):
    """
    Control the submission of writes to DynamoDB
    """

    def __init__(self, dynamo_table_name: str, partition_key: str, partition_key_data_type: type = str,
                 max_batch_size: int = 25, flush_payload_on_max_batch_size: bool = True):
        self.dynamo_table_name = dynamo_table_name
        self.partition_key = partition_key
        self.partition_key_data_type = partition_key_data_type
        super().__init__('dynamodb', batch_dispatch_method='batch_write_item', batch_size=max_batch_size,
                         flush_payload_on_max_batch_size=flush_payload_on_max_batch_size)
        self.dynamo_table = self._aws_service.Table(self.dynamo_table_name)

    def _send_individual_payload(self, payload: dict, retry: int = 4):
        """
        Write an individual record to Dynamo
        :param payload: JSON representation of a new record to write to the Dynamo table
        """
        logger.debug(f"Attempting to send individual payload ({retry} retries left): {payload}")
        try:
            self.dynamo_table.put_item(Item=payload)
        except ClientError as e:
            if retry:
                logger.debug(f"Individual send attempt has failed, retrying: {str(e)}")
                self._send_individual_payload(payload, retry - 1)
            else:
                logger.error(f"Individual send attempt has failed, no more retries remaining: {str(e)}")
                logger.debug(f"Failed payload: {payload}")
                raise

    def _process_batch_send_response(self, response: dict):
        """
        Parse the response from a batch_write call, handle any failures as required.
        :param response: Response JSON from a batch_write_item request
        """
        unprocessed_items = response['UnprocessedItems']
        if unprocessed_items:
            logger.warning(f"Batch write failed to write all items, "
                           f"{len(unprocessed_items[self.dynamo_table_name])} were rejected")
            for item in unprocessed_items[self.dynamo_table_name]:
                if 'PutRequest' in item:
                    self._send_individual_payload(item['PutRequest']['Item'])
                else:
                    raise TypeError("Individual write type is not supported")

    def _batch_send_payloads(self, batch: dict = None, **kwargs):
        """
        Submit the batch to DynamoDB
        """
        if 'retry' in kwargs:
            super()._batch_send_payloads(batch, kwargs['retry'])
        else:
            super()._batch_send_payloads({'RequestItems': {self.dynamo_table_name: batch}})

    def flush_payloads(self):
        """
        Send any records remaining in the current batch bucket
        """
        super().flush_payloads()

    def submit_payload(self, payload, partition_key_location: str = "Id"):
        """
        Submit a record ready for batch sending to DynamoDB
        """
        logger.debug(f"Payload submitted to {self._aws_service_name} dispatcher: {payload}")
        if self.partition_key not in payload.keys():
            payload[self.partition_key] = self.partition_key_data_type(payload[partition_key_location])
        if not any(d["PutRequest"]["Item"][self.partition_key] == payload[self.partition_key]
                   for d in self._payload_list):
            super().submit_payload({
                "PutRequest": {
                    "Item": convert_floats_in_dict_to_decimals(payload)
                }
            })
        else:
            logger.warning("The candidate payload has a primary_partition_key which already exists in the "
                           f"payload_list: {payload}")
        self._flush_payload_selector()
