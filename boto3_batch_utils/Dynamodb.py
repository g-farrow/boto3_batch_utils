import logging
from botocore.exceptions import ClientError

from boto3_batch_utils.Base import BaseDispatcher
from boto3_batch_utils.utils import convert_floats_in_dict_to_decimals
from boto3_batch_utils.constants import DYNAMODB_BATCH_MAX_BYTES, DYNAMODB_BATCH_MAX_PAYLOADS, \
    DYNAMODB_MESSAGE_MAX_BYTES

logger = logging.getLogger('boto3-batch-utils')


class DynamoBatchDispatcher(BaseDispatcher):
    """
    Control the submission of writes to DynamoDB
    """

    def __init__(self, dynamo_table_name: str, partition_key: str, sort_key: str = None,
                 partition_key_data_type: type = str, max_batch_size: int = 25,
                 flush_payload_on_max_batch_size: bool = True):
        self.dynamo_table_name = dynamo_table_name
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.partition_key_data_type = partition_key_data_type
        self._dynamo_table = None
        super().__init__('dynamodb', batch_dispatch_method='batch_write_item', max_batch_size=max_batch_size,
                         flush_payload_on_max_batch_size=flush_payload_on_max_batch_size)
        self._aws_service_batch_max_payloads = DYNAMODB_BATCH_MAX_PAYLOADS
        self._aws_service_message_max_bytes = DYNAMODB_MESSAGE_MAX_BYTES
        self._aws_service_batch_max_bytes = DYNAMODB_BATCH_MAX_BYTES
        self._batch_payload = {'RequestItems': {self.dynamo_table_name: []}}
        self._validate_initialisation()

    def __str__(self):
        return f"DynamoBatchDispatcher::{self.dynamo_table_name}"

    def _initialise_aws_client(self):
        """
        Initialise client/resource for the AWS service
        """
        super()._initialise_aws_client()
        self._dynamo_table = self._aws_service.Table(self.dynamo_table_name)

    def _send_individual_payload(self, payload: dict, retry: int = 4):
        """
        Write an individual record to Dynamo
        :param payload: JSON representation of a new record to write to the Dynamo table
        """
        logger.debug(f"Attempting to send individual payload ({retry} retries left): {payload}")
        try:
            self._dynamo_table.put_item(Item=payload)
        except ClientError as e:
            if retry:
                logger.debug(f"Individual send attempt has failed, retrying: {str(e)}")
                self._send_individual_payload(payload, retry - 1)
            else:
                logger.error(f"Individual send attempt has failed, no more retries remaining: {str(e)}")
                logger.debug(f"Failed payload: {payload}")
                raise e

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

    def _append_payload_to_current_batch(self, payload):
        """ Append the payload to the service specific batch structure """
        self._batch_payload['RequestItems'][self.dynamo_table_name].append(payload)

    def submit_payload(self, payload, partition_key_location: str = "Id"):
        """
        Submit a record ready for batch sending to DynamoDB
        """
        logger.debug(f"Payload submitted to {self.aws_service_name} dispatcher: {payload}")
        if self.partition_key not in payload.keys():
            payload[self.partition_key] = self.partition_key_data_type(payload[partition_key_location])
        if self._check_payload_is_unique(payload):
            super().submit_payload({
                "PutRequest": {
                    "Item": convert_floats_in_dict_to_decimals(payload)
                }
            })
        else:
            logger.warning("The candidate payload has a primary_partition_key which already exists in the "
                           f"payload_list: {payload}")
        self._flush_payload_selector()

    def _check_payload_is_unique(self, payload: dict) -> bool:
        """
        Check that a payload is unique, according to the partition key (and sort key if applicable)
        """
        logger.debug("Checking if the payload already exists in the existing batch")
        if self.sort_key:
            return self._check_payload_is_unique_by_partition_key_and_sort_key(payload)
        else:
            return self._check_payload_is_unique_by_partition_key(payload)

    def _check_payload_is_unique_by_partition_key(self, payload: dict) -> bool:
        """
        Use the partition key within the submitted payload to determine the payloads uniqueness, compared to existing
        payloads in the batch
        """
        logger.debug("Checking if the partition key already exists in the existing batch")
        if any(d["PutRequest"]["Item"][self.partition_key] == payload[self.partition_key]
               for d in self._batch_payload):
            logger.debug("This payload has already been submitted")
            return False
        else:
            logger.debug("This payload is unique")
            return True

    def _check_payload_is_unique_by_partition_key_and_sort_key(self, payload: dict) -> bool:
        """
        Use the partition key AND sort key within the submitted payload to determine the payloads uniqueness,
        compared to existing payloads in the batch
        """
        logger.debug("Checking if the partition key, sort key combination already exists in the existing batch")
        if any(
                (d["PutRequest"]["Item"][self.partition_key] == payload[self.partition_key] and
                 d["PutRequest"]["Item"][self.sort_key] == payload[self.sort_key])
                for d in self._batch_payload
        ):
            logger.debug("This payload has already been submitted")
            return False
        else:
            logger.debug("This payload is unique")
            return True
