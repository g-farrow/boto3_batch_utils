from unittest import TestCase
from unittest.mock import patch, Mock, call

from boto3_batch_utils.Dynamodb import DynamoBatchDispatcher, TypeSerializer
from boto3_batch_utils.Base import BaseDispatcher


class MockClient:

    def __init__(self, client_name):
        self.client_name = client_name + "_client"

    def batch_write_item(self):
        pass

    def put_item(self):
        pass


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.utils.convert_floats_in_dict_to_decimals')
@patch.object(BaseDispatcher, 'submit_payload')
class SubmitPayload(TestCase):

    def test_where_key_preexists(self, mock_submit_payload, mock_convert_decimals):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        test_payload = {'p_key': 1}
        mock_convert_decimals.return_value = test_payload
        dy.submit_payload(test_payload, partition_key_location=None)
        mock_submit_payload.assert_called_once_with({"PutRequest": {"Item": test_payload}})


    def test_where_key_requires_mapping(self, mock_submit_payload, mock_convert_decimals):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        test_payload = {'unmapped_id': 1}
        mock_convert_decimals.return_value = test_payload
        dy.submit_payload(test_payload, partition_key_location='unmapped_id')
        mock_submit_payload.assert_called_once_with({"PutRequest": {"Item": test_payload}})

    def test_where_key_not_found(self, mock_submit_payload, mock_convert_decimals):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        test_payload = {'there_is_no_real_id_here': 1}
        mock_convert_decimals.return_value = test_payload
        with self.assertRaises(KeyError):
            dy.submit_payload(test_payload, partition_key_location='something_useless')
        mock_submit_payload.assert_not_called()


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, 'flush_payloads')
class FlushPayloads(TestCase):

    def test(self, mock_flush_payloads):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        dy.flush_payloads()
        mock_flush_payloads.assert_called_once_with()


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, '_batch_send_payloads')
class BatchSendPayloads(TestCase):

    def test(self, mock_batch_send_payloads):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        test_batch = "a_test"
        dy._batch_send_payloads(test_batch)
        mock_batch_send_payloads.assert_called_once_with({'RequestItems': {'test_table_name': test_batch}})


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, '_process_batch_send_response')
class ProcessBatchSendResponse(TestCase):

    def test_no_unprocessed_items(self, mock_base_process_batch_send_response):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        dy._send_individual_payload = Mock()
        test_response = {'UnprocessedItems': []}
        dy._process_batch_send_response(test_response)
        mock_base_process_batch_send_response.assert_not_called()
        dy._send_individual_payload.assert_not_called()

    def test_one_unprocessed_item(self, mock_base_process_batch_send_response):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        dy._send_individual_payload = Mock()
        test_response = {'UnprocessedItems': {'test_table_name': ["TEST_ITEM"]}}
        dy._process_batch_send_response(test_response)
        mock_base_process_batch_send_response.assert_not_called()
        dy._send_individual_payload.assert_called_once_with("TEST_ITEM")

    def test_several_unprocessed_items(self, mock_base_process_batch_send_response):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        dy._send_individual_payload = Mock()
        test_response = {'UnprocessedItems': {'test_table_name': ["TEST_ITEM1", "TEST_ITEM2", "TEST_ITEM3"]}}
        dy._process_batch_send_response(test_response)
        mock_base_process_batch_send_response.assert_not_called()
        dy._send_individual_payload.assert_has_calls([
            call("TEST_ITEM1"),
            call("TEST_ITEM2"),
            call("TEST_ITEM3")
        ])


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(TypeSerializer, 'serialize')
@patch.object(BaseDispatcher, '_send_individual_payload')
class SendIndividualPayload(TestCase):

    def test(self, mock_send_individual_payload, mock_serialize):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        test_payload = "unprocessed_payload"
        mock_serialize.return_value = "processed_payload"
        dy._send_individual_payload(test_payload)
        mock_send_individual_payload.assert_called_once_with("processed_payload", retry=4)
