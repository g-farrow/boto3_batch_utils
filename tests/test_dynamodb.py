from unittest import TestCase
from unittest.mock import patch, Mock, call

from botocore.exceptions import ClientError

from boto3_batch_utils.Dynamodb import DynamoBatchDispatcher
from boto3_batch_utils.Base import BaseDispatcher


class MockClient:

    def __init__(self, client_name):
        self.client_name = client_name + "_client"

    def batch_write_item(self):
        pass

    def put_item(self):
        pass


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
@patch('boto3_batch_utils.utils.convert_floats_in_dict_to_decimals')
@patch.object(BaseDispatcher, 'submit_payload')
class SubmitPayload(TestCase):

    def test_where_key_preexists(self, mock_submit_payload, mock_convert_decimals):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        mock_check_payload_is_unique = Mock(return_value=True)
        dy._check_payload_is_unique = mock_check_payload_is_unique
        test_payload = {'p_key': 1}
        mock_convert_decimals.return_value = test_payload
        dy.submit_payload(test_payload)
        mock_check_payload_is_unique.assert_called_once_with(test_payload)
        mock_submit_payload.assert_called_once_with({"PutRequest": {"Item": test_payload}})

    def test_where_key_requires_mapping(self, mock_submit_payload, mock_convert_decimals):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        mock_check_payload_is_unique = Mock(return_value=True)
        dy._check_payload_is_unique = mock_check_payload_is_unique
        test_payload = {'unmapped_id': 1}
        mock_convert_decimals.return_value = test_payload
        dy.submit_payload(test_payload, partition_key_location='unmapped_id')
        mock_check_payload_is_unique.assert_called_once_with(test_payload)
        mock_submit_payload.assert_called_once_with({"PutRequest": {"Item": test_payload}})

    def test_where_key_not_found(self, mock_submit_payload, mock_convert_decimals):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        mock_check_payload_is_unique = Mock(return_value=True)
        dy._check_payload_is_unique = mock_check_payload_is_unique
        test_payload = {'there_is_no_real_id_here': 1}
        mock_convert_decimals.return_value = test_payload
        with self.assertRaises(KeyError):
            dy.submit_payload(test_payload, partition_key_location='something_useless')
        mock_check_payload_is_unique.assert_not_called()
        mock_submit_payload.assert_not_called()

    def test_where_payload_is_a_duplicate(self, mock_submit_payload, mock_convert_decimals):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        mock_check_payload_is_unique = Mock(return_value=False)
        dy._check_payload_is_unique = mock_check_payload_is_unique
        test_payload = {'p_key': 1}
        mock_convert_decimals.return_value = test_payload
        dy.submit_payload(test_payload)
        mock_check_payload_is_unique.assert_called_once_with(test_payload)
        mock_submit_payload.assert_not_called()


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
class TestCheckPayloadIsUnique(TestCase):

    def test_sort_key_given_and_is_not_duplicate(self):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False,
                                   sort_key="sorted")
        dy._check_payload_is_unique_by_partition_key_and_sort_key = Mock(return_value=True)
        dy._check_payload_is_unique_by_partition_key = Mock()
        test_payload = {'test': True}

        self.assertTrue(dy._check_payload_is_unique(test_payload))

        dy._check_payload_is_unique_by_partition_key_and_sort_key.assert_called_once_with(test_payload)
        dy._check_payload_is_unique_by_partition_key.assert_not_called()

    def test_sort_key_given_and_is_a_duplicate(self):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False,
                                   sort_key="sorted")
        dy._check_payload_is_unique_by_partition_key_and_sort_key = Mock(return_value=False)
        dy._check_payload_is_unique_by_partition_key = Mock()
        test_payload = {'test': True}

        self.assertFalse(dy._check_payload_is_unique(test_payload))

        dy._check_payload_is_unique_by_partition_key_and_sort_key.assert_called_once_with(test_payload)
        dy._check_payload_is_unique_by_partition_key.assert_not_called()

    def test_sort_key_not_given_and_is_not_duplicate(self):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        dy._check_payload_is_unique_by_partition_key_and_sort_key = Mock()
        dy._check_payload_is_unique_by_partition_key = Mock(return_value=True)
        test_payload = {'test': True}

        self.assertTrue(dy._check_payload_is_unique(test_payload))

        dy._check_payload_is_unique_by_partition_key_and_sort_key.assert_not_called()
        dy._check_payload_is_unique_by_partition_key.assert_called_once_with(test_payload)

    def test_sort_key_not_given_and_is_a_duplicate(self):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        dy._check_payload_is_unique_by_partition_key_and_sort_key = Mock()
        dy._check_payload_is_unique_by_partition_key = Mock(return_value=False)
        test_payload = {'test': True}

        self.assertFalse(dy._check_payload_is_unique(test_payload))

        dy._check_payload_is_unique_by_partition_key_and_sort_key.assert_not_called()
        dy._check_payload_is_unique_by_partition_key.assert_called_once_with(test_payload)


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
class TestCheckPayloadIsUniqueByPartitionKey(TestCase):

    def test_empty_batch(self):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        dy._batch_payload = []

        test_payload = {'p_key': 'abc'}

        self.assertTrue(dy._check_payload_is_unique_by_partition_key(test_payload))

    def test_record_already_in_batch(self):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        dy._batch_payload = [{'PutRequest': {'Item': {'p_key': 'abc'}}}]

        test_payload = {'p_key': 'abc'}

        self.assertFalse(dy._check_payload_is_unique_by_partition_key(test_payload))

    def test_record_not_in_batch(self):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        dy._batch_payload = [{'PutRequest': {'Item': {'p_key': 'cde'}}}]

        test_payload = {'p_key': 'abc'}

        self.assertTrue(dy._check_payload_is_unique_by_partition_key(test_payload))


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
class TestCheckPayloadIsUniqueByPartitionKeyAndSortKey(TestCase):

    def test_empty_batch(self):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False,
                                   sort_key='s_key')
        dy._batch_payload = []

        test_payload = {'p_key': 'abc', 's_key': 'def'}

        self.assertTrue(dy._check_payload_is_unique_by_partition_key_and_sort_key(test_payload))

    def test_sort_key_in_batch_partition_key_is_not(self):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False,
                                   sort_key='s_key')
        dy._batch_payload = [{'PutRequest': {'Item': {'p_key': 'cde', 's_key': 'def'}}}]

        test_payload = {'p_key': 'abc', 's_key': 'def'}

        self.assertTrue(dy._check_payload_is_unique_by_partition_key_and_sort_key(test_payload))

    def test_sort_key_not_in_batch_partition_key_is(self):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False,
                                   sort_key='s_key')
        dy._batch_payload = [{'PutRequest': {'Item': {'p_key': 'abc', 's_key': 'ghi'}}}]

        test_payload = {'p_key': 'abc', 's_key': 'def'}

        self.assertTrue(dy._check_payload_is_unique_by_partition_key_and_sort_key(test_payload))

    def test_sort_key_and_partition_key_in_batch(self):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False,
                                   sort_key='s_key')
        dy._batch_payload = [{'PutRequest': {'Item': {'p_key': 'abc', 's_key': 'def'}}}]

        test_payload = {'p_key': 'abc', 's_key': 'def'}

        self.assertFalse(dy._check_payload_is_unique_by_partition_key_and_sort_key(test_payload))


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
@patch.object(BaseDispatcher, 'flush_payloads')
class FlushPayloads(TestCase):

    def test(self, mock_flush_payloads):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        dy.flush_payloads()
        mock_flush_payloads.assert_called_once_with()


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
@patch.object(BaseDispatcher, '_batch_send_payloads')
class BatchSendPayloads(TestCase):

    def test(self, mock_batch_send_payloads):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        test_batch = {'a_test': True}
        dy._batch_send_payloads(test_batch)
        mock_batch_send_payloads.assert_called_once_with({'RequestItems': {'test_table_name': test_batch}})


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
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
        test_response = {'UnprocessedItems': {'test_table_name': [{"PutRequest": {"Item": "TEST_ITEM"}}]}}
        dy._process_batch_send_response(test_response)
        mock_base_process_batch_send_response.assert_not_called()
        dy._send_individual_payload.assert_called_once_with("TEST_ITEM")

    def test_several_unprocessed_items(self, mock_base_process_batch_send_response):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        dy._send_individual_payload = Mock()
        test_response = {'UnprocessedItems': {
            'test_table_name': [
                {"PutRequest": {"Item": "TEST_ITEM1"}},
                {"PutRequest": {"Item": "TEST_ITEM2"}},
                {"PutRequest": {"Item": "TEST_ITEM3"}}
            ]
        }}
        dy._process_batch_send_response(test_response)
        mock_base_process_batch_send_response.assert_not_called()
        dy._send_individual_payload.assert_has_calls([
            call("TEST_ITEM1"),
            call("TEST_ITEM2"),
            call("TEST_ITEM3")
        ])


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
class SendIndividualPayload(TestCase):

    def test_happy_path(self):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        dy._dynamo_table = Mock()
        dy._dynamo_table.put_item = Mock()
        test_payload = {"processed_payload": False}
        dy._send_individual_payload(test_payload)
        dy._dynamo_table.put_item.assert_called_once_with(**{'Item': test_payload})

    def test_client_error_retries_remaining(self):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        dy._dynamo_table = Mock()
        dy._dynamo_table.put_item.side_effect = [ClientError({'Error': {'Code': 500, 'Message': 'broken'}}, "Dynamo"),
                                                 None]
        test_payload = {"processed_payload": False}
        dy._send_individual_payload(test_payload, retry=1)
        dy._dynamo_table.put_item.assert_has_calls([call(**{'Item': test_payload}), call(**{'Item': test_payload})])

    def test_client_error_no_retries_remaining(self):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        dy._dynamo_table = Mock()
        dy._dynamo_table.put_item.side_effect = [ClientError({'Error': {'Code': 500, 'Message': 'broken'}}, "Dynamo")]
        test_payload = {"processed_payload": False}
        with self.assertRaises(ClientError) as context:
            dy._send_individual_payload(test_payload, retry=0)
        dy._dynamo_table.put_item.assert_called_once_with(**{'Item': test_payload})


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
@patch.object(BaseDispatcher, '_initialise_aws_client')
class TestInitialiseAwsClient(TestCase):

    def test(self, mock_initialise_aws_client):
        dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
        dy._aws_service = Mock()
        dy._aws_service.Table = Mock(return_value="test table")

        table_client = dy._initialise_aws_client()

        mock_initialise_aws_client.assert_called_once()
        dy._aws_service.Table.assert_called_once_with('test_table_name')
        self.assertEqual('test table', dy._dynamo_table)
