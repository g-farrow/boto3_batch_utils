from unittest import TestCase
from unittest.mock import patch, Mock, call

from boto3_batch_utils import DynamoBatchDispatcher


@patch('boto3_batch_utils.Base.boto3', Mock())
class TestSqsStandard(TestCase):

    def test_more_than_one_batch_small_messages(self):
        dy_client = DynamoBatchDispatcher(dynamo_table_name='test_table', partition_key='id', max_batch_size=5)

        mock_boto3 = Mock()
        dy_client._aws_service = mock_boto3
        dy_client._batch_dispatch_method = Mock(return_value={'UnprocessedItems': {}})

        for i in range(0, 6):
            dy_client.submit_payload({"id": f"abc{i}", "message": True})

        dy_client.flush_payloads()

        self.assertEqual(2, dy_client._batch_dispatch_method.call_count)

    def test_batch_of_10_failed_first_time_messages(self):
        dy_client = DynamoBatchDispatcher(dynamo_table_name='test_table', partition_key='m_id', max_batch_size=10)

        mock_boto3 = Mock()
        dy_client._aws_service = mock_boto3
        dy_client._dynamo_table = Mock()

        test_payloads = [
            {'m_id': 1, 'message': 'message contents 1'},
            {'m_id': 2, 'message': 'message contents 2'},
            {'m_id': 3, 'message': 'message contents 3'},
            {'m_id': 4, 'message': 'message contents 4'},
            {'m_id': 5, 'message': 'message contents 5'},
            {'m_id': 6, 'message': 'message contents 6'},
            {'m_id': 7, 'message': 'message contents 7'},
            {'m_id': 8, 'message': 'message contents 8'},
            {'m_id': 9, 'message': 'message contents 9'},
            {'m_id': 10, 'message': 'message contents 10'}
        ]

        failure_response = {'UnprocessedItems': {'test_table': []}}
        for pl in test_payloads:
            failure_response['UnprocessedItems']['test_table'].append({'PutRequest': {'Item': pl}})

        dy_client._batch_dispatch_method = Mock(side_effect=[failure_response, True, True])
        dy_client._individual_dispatch_method = Mock()

        for test_payload in test_payloads:
            dy_client.submit_payload(test_payload)

        dy_client.flush_payloads()
        dy_client._batch_dispatch_method.assert_called_once()
        dy_client._dynamo_table.put_item.assert_has_calls([
            call(**{'Item': {'m_id': 1, 'message': 'message contents 1'}}),
            call(**{'Item': {'m_id': 2, 'message': 'message contents 2'}}),
            call(**{'Item': {'m_id': 3, 'message': 'message contents 3'}}),
            call(**{'Item': {'m_id': 4, 'message': 'message contents 4'}}),
            call(**{'Item': {'m_id': 5, 'message': 'message contents 5'}}),
            call(**{'Item': {'m_id': 6, 'message': 'message contents 6'}}),
            call(**{'Item': {'m_id': 7, 'message': 'message contents 7'}}),
            call(**{'Item': {'m_id': 8, 'message': 'message contents 8'}}),
            call(**{'Item': {'m_id': 9, 'message': 'message contents 9'}}),
            call(**{'Item': {'m_id': 10, 'message': 'message contents 10'}}),
        ], any_order=True)
