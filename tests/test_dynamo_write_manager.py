from boto3_batch_utils.dynamo_batch_write_manager import DynamoWriteManager, dynamodb_batch_write_limit
import unittest
from unittest.mock import patch, MagicMock, call

test_config = {"message_type": {
    "Source_to_Target": {
        "target_record_key": "partition_key",
        "target_entity_table": "ParentTableName",
        "transformations": [
            {"source_key": "a", "target_key": "b", "transformation_type": "c"}
        ]
    }
}}


# class SendIndividualRecord(unittest.TestCase):
#     """
#     Check that a selection of records, with a mix of matching and mismatching parent id's are added to the correct
#     group
#     """
#
#     @patch.dict(environ, {'TRANSFORMATION_TYPE': 'Source_to_Target'})
#     @patch('src.hera.hera.config', test_config)
#     @patch('src.hera.hera.target_entity_table')
#     def test_successfully_put_record(self, target_entity_table):
#         """
#         Successfully put a record to Dynamo
#         """
#         target_entity_table.put_item.return_value = ""
#         dwm = DynamoWriteManager()
#         record = {"PutRequest": {"Item": {"partition_key": "x"}}}
#         dwm._send_individual_record(record)
#         target_entity_table.put_item.assert_called_once_with(Item={"Id": "x", "partition_key": "x"})


class ProcessBatchWriteResponse(unittest.TestCase):
    """
    Check the handling of unprocessed items from a batch write
    """

    def test_when_unprocessed_list_is_empty_nothing_is_sent_to_dynamo(self):
        """
        If the response of the batch write does not contain unprocessed records, nothing is reprocessed
        """
        sir = MagicMock()
        dwm = DynamoWriteManager(dynamo_table_name="ParentTableName", primary_partition_key="partition_key")
        dwm._send_individual_record = sir
        response = {'UnprocessedItems': []}
        dwm._process_batch_write_response(response)
        sir.assert_not_called()

    def test_1_record_is_reprocessed_if_it_was_unprocessed(self):
        """
        If the response of the batch write contains 1 unprocessed records, that record is reprocessed
        """
        sir = MagicMock()
        dwm = DynamoWriteManager(dynamo_table_name="ParentTableName", primary_partition_key="partition_key")
        dwm._send_individual_record = sir
        response = {'UnprocessedItems': {'ParentTableName': ["1"]}}
        dwm._process_batch_write_response(response)
        sir.assert_called_once_with("1")

    def test_2_record_are_reprocessed_if_2_were_unprocessed(self):
        """
        If the response of the batch write contains 2 unprocessed records, those records are reprocessed
        """
        sir = MagicMock()
        dwm = DynamoWriteManager(dynamo_table_name="ParentTableName", primary_partition_key="partition_key")
        dwm._send_individual_record = sir
        response = {'UnprocessedItems': {'ParentTableName': ["1", "2"]}}
        dwm._process_batch_write_response(response)
        sir.assert_has_calls([call("1"), call("2")])


class WriteBatchToDynamo(unittest.TestCase):
    """
    Check the batch writing of records to Dynamo
    """

    @patch('boto3_batch_utils.dynamo_batch_write_manager.dynamodb.batch_write_item')
    def test_a_batch_of_1_record_is_constructed_and_submitted_correctly(self, batch_write_item):
        """
        When the batch contains just 1 record, the payload is compiled correctly and submitted to dynamo batch write
        """
        batch_write_item.return_value = {'UnprocessedItems': {}}
        batch = ["1"]
        dwm = DynamoWriteManager(dynamo_table_name="ParentTableName", primary_partition_key="partition_key")
        dwm._process_batch_write_response = lambda response: response
        dwm._write_batch_to_dynamo(batch)
        batch_write_item.assert_called_once_with(RequestItems={'ParentTableName': batch})

    @patch('boto3_batch_utils.dynamo_batch_write_manager.dynamodb.batch_write_item')
    def test_a_batch_of_2_records_is_constructed_and_submitted_correctly(self, batch_write_item):
        """
        When the batch contains just 2 records, the payload is compiled correctly and submitted to dynamo batch write
        """
        batch_write_item.return_value = {'UnprocessedItems': {}}
        batch = ["1", "2"]
        dwm = DynamoWriteManager(dynamo_table_name="ParentTableName", primary_partition_key="partition_key")
        dwm._process_batch_write_response = lambda response: response
        dwm._write_batch_to_dynamo(batch)
        batch_write_item.assert_called_once_with(RequestItems={'ParentTableName': batch})

    @patch('boto3_batch_utils.dynamo_batch_write_manager.dynamodb.batch_write_item')
    def test_a_batch_of_25_records_is_constructed_and_submitted_correctly(self, batch_write_item):
        """
        When the batch contains 25 records, the payload is compiled correctly and submitted to dynamo batch write
        """
        batch_write_item.return_value = {'UnprocessedItems': {}}
        batch = []
        for x in range(1, 26):
            batch.append(str(x))
        dwm = DynamoWriteManager(dynamo_table_name="ParentTableName", primary_partition_key="partition_key")
        dwm._process_batch_write_response = lambda response: response
        dwm._write_batch_to_dynamo(batch)
        batch_write_item.assert_called_once_with(RequestItems={'ParentTableName': batch})


class SubmitRecord(unittest.TestCase):
    """
    Check the functional behaviour of submitting a record to the manager
    """

    def test_submit_the_first_record_to_the_dwm(self):
        """
        When the DWM has no current 'records_to_write', submit a new record.
        """
        dwm = DynamoWriteManager(dynamo_table_name="ParentTableName", primary_partition_key="partition_key")
        record = {"partition_key": "1"}
        dwm.submit_record(record)
        self.assertEqual(1, len(dwm.records_to_write), msg="Ensure only 1 record exists in the records_to_write list")
        self.assertEqual({"PutRequest": {"Item": record}},
                         dwm.records_to_write[0], msg="Ensure the record submitted has been modified correctly")

    def test_submit_the_second_record_to_the_dwm(self):
        """
        When the DWM has no current 'records_to_write', submit a new record.
        """
        dwm = DynamoWriteManager(dynamo_table_name="ParentTableName", primary_partition_key="partition_key")
        existing_record = {"Id": "999", "partition_key": "999"}
        dwm.records_to_write.append({"PutRequest": {"Item": existing_record}})
        record = {"partition_key": "1"}
        dwm.submit_record(record)
        self.assertEqual(2, len(dwm.records_to_write),
                         msg="Ensure the existing record is in records_to_write list as well as the new record")
        self.assertEqual({"PutRequest": {"Item": existing_record}},
                         dwm.records_to_write[0], msg="Ensure the existing record has not been modified")
        self.assertEqual({"PutRequest": {"Item": record}},
                         dwm.records_to_write[1], msg="Ensure the record submitted has been modified correctly")


class WriteRecords(unittest.TestCase):
    """
    Check the functional behaviour of writing records to dynamo
    """

    @patch('boto3_batch_utils.dynamo_batch_write_manager.chunks')
    def test_there_are_no_records_to_write(self, chunks):
        """
        When the DWM has no current 'records_to_write' it does not attempt to send data to dynamo.
        """
        dwm = DynamoWriteManager(dynamo_table_name="ParentTableName", primary_partition_key="partition_key")
        mock_write_batch_to_dynamo = MagicMock()
        dwm._write_batch_to_dynamo = mock_write_batch_to_dynamo
        dwm.write_records()
        chunks.assert_not_called()
        mock_write_batch_to_dynamo.assert_not_called()

    @patch('boto3_batch_utils.dynamo_batch_write_manager.chunks')
    def test_there_is_1_record_to_write(self, chunks):
        """
        When the DWM has 1 'records_to_write' the write request is made correctly.
        """
        dwm = DynamoWriteManager(dynamo_table_name="ParentTableName", primary_partition_key="partition_key")
        mock_write_batch_to_dynamo = MagicMock()
        dwm._write_batch_to_dynamo = mock_write_batch_to_dynamo
        record_1 = {"PutRequest": {"Item": {"Id": "1", "partition_key": "1"}}}
        chunks.return_value = [[record_1]]
        dwm.records_to_write.append(record_1)
        dwm.write_records()
        chunks.assert_called_once_with([record_1], dynamodb_batch_write_limit)
        dwm._write_batch_to_dynamo.assert_called_once_with([record_1])

    @patch('boto3_batch_utils.dynamo_batch_write_manager.chunks')
    def test_there_is_a_maximum_batch_size_to_write(self, chunks):
        """
        When the DWM has a full batch of 'records_to_write' the write request is made correctly.
        """
        dwm = DynamoWriteManager(dynamo_table_name="ParentTableName", primary_partition_key="partition_key")
        mock_write_batch_to_dynamo = MagicMock()
        dwm._write_batch_to_dynamo = mock_write_batch_to_dynamo
        test_records_to_write = []
        for x in range(1, dynamodb_batch_write_limit+1):
            test_records_to_write.append({"PutRequest": {"Item": {"Id": str(x), "partition_key": str(x)}}})
        chunks.return_value = [test_records_to_write]
        dwm.records_to_write += test_records_to_write
        dwm.write_records()
        chunks.assert_called_once_with(test_records_to_write, dynamodb_batch_write_limit)
        dwm._write_batch_to_dynamo.assert_called_once_with(test_records_to_write)

    @patch('boto3_batch_utils.dynamo_batch_write_manager.chunks')
    def test_all_records_are_sent_when_there_are_multiple_batches_1(self, chunks):
        """
        When the DWM has more than 1 batch of 'records_to_write' all the write requests are made correctly.
        """
        dwm = DynamoWriteManager(dynamo_table_name="ParentTableName", primary_partition_key="partition_key")
        mock_write_batch_to_dynamo = MagicMock()
        dwm._write_batch_to_dynamo = mock_write_batch_to_dynamo
        test_records_to_write_batch_1 = []
        for x in range(1, dynamodb_batch_write_limit + 1):
            test_records_to_write_batch_1.append({"PutRequest": {"Item": {"Id": str(x), "partition_key": str(x)}}})
        test_records_to_write_batch_2 = []
        dwm.records_to_write += test_records_to_write_batch_1
        for x in range(dynamodb_batch_write_limit+1, (2*dynamodb_batch_write_limit) + 1):
            test_records_to_write_batch_2.append({"PutRequest": {"Item": {"Id": str(x), "partition_key": str(x)}}})
        dwm.records_to_write += test_records_to_write_batch_2
        chunks.return_value = [test_records_to_write_batch_1, test_records_to_write_batch_2]
        dwm.write_records()
        chunks.assert_called_once_with(test_records_to_write_batch_1+test_records_to_write_batch_2,
                                       dynamodb_batch_write_limit)
        dwm._write_batch_to_dynamo.assert_has_calls([call(test_records_to_write_batch_1),
                                                     call(test_records_to_write_batch_2)])
