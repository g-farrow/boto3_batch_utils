from unittest import TestCase
from unittest.mock import patch, Mock, call
from copy import deepcopy

from boto3_batch_utils import KinesisBatchDispatcher


@patch('boto3_batch_utils.Base.boto3', Mock())
class TestKinesis(TestCase):

    def test_more_than_one_batch_small_messages(self):
        kinesis_client = KinesisBatchDispatcher(stream_name='test_stream', max_batch_size=30)

        mock_boto3 = Mock()
        kinesis_client._aws_service = mock_boto3
        kinesis_client._batch_dispatch_method = Mock(return_value={'FailedRecordCount': 0})

        test_payload = {'Id': 123, 'message': True}

        for _ in range(0, 31):
            kinesis_client.submit_payload(test_payload)

        kinesis_client.flush_payloads()

        self.assertEqual(2, kinesis_client._batch_dispatch_method.call_count)

    def test_batch_of_10_failed_first_time_messages(self):
        kinesis_client = KinesisBatchDispatcher(stream_name='test_stream', partition_key_identifier='m_id')

        mock_boto3 = Mock()
        kinesis_client._aws_service = mock_boto3

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

        #  All records fail in first attempt
        failure_response_1 = {
            'FailedRecordCount': 10,
            'Records': [
                {'m_id': 1, 'message': 'message contents 1', 'ErrorCode': 'badness'},
                {'m_id': 2, 'message': 'message contents 2', 'ErrorCode': 'badness'},
                {'m_id': 3, 'message': 'message contents 3', 'ErrorCode': 'badness'},
                {'m_id': 4, 'message': 'message contents 4', 'ErrorCode': 'badness'},
                {'m_id': 5, 'message': 'message contents 5', 'ErrorCode': 'badness'},
                {'m_id': 6, 'message': 'message contents 6', 'ErrorCode': 'badness'},
                {'m_id': 7, 'message': 'message contents 7', 'ErrorCode': 'badness'},
                {'m_id': 8, 'message': 'message contents 8', 'ErrorCode': 'badness'},
                {'m_id': 9, 'message': 'message contents 9', 'ErrorCode': 'badness'},
                {'m_id': 10, 'message': 'message contents 10', 'ErrorCode': 'badness'}
            ],
            'EncryptionType': 'NONE'
        }

        # first 5 succeed, second 5 fail
        failure_response_2 = {
            'FailedRecordCount': 5,
            'Records': [
                {'m_id': 6, 'message': 'message contents 6', 'ErrorCode': 'badness'},
                {'m_id': 7, 'message': 'message contents 7', 'ErrorCode': 'badness'},
                {'m_id': 8, 'message': 'message contents 8', 'ErrorCode': 'badness'},
                {'m_id': 9, 'message': 'message contents 9', 'ErrorCode': 'badness'},
                {'m_id': 10, 'message': 'message contents 10', 'ErrorCode': 'badness'}
            ],
            'EncryptionType': 'NONE'
        }

        # 6, 7 and 8 succeed, 9 and 10 fail
        failure_response_3 = {
            'FailedRecordCount': 2,
            'Records': [
                {'m_id': 9, 'message': 'message contents 9', 'ErrorCode': 'badness'},
                {'m_id': 10, 'message': 'message contents 10', 'ErrorCode': 'badness'}
            ],
            'EncryptionType': 'NONE'
        }

        kinesis_client._batch_dispatch_method = Mock(side_effect=[failure_response_1, failure_response_2,
                                                                  failure_response_3])
        kinesis_client._individual_dispatch_method = Mock()

        for test_payload in test_payloads:
            kinesis_client.submit_payload(test_payload)

        kinesis_client.flush_payloads()

        kinesis_client._batch_dispatch_method.assert_has_calls([
            call(**{'Records': [
                {'Data': '{"m_id": 1, "message": "message contents 1"}', 'PartitionKey': '1'},
                {'Data': '{"m_id": 2, "message": "message contents 2"}', 'PartitionKey': '2'},
                {'Data': '{"m_id": 3, "message": "message contents 3"}', 'PartitionKey': '3'},
                {'Data': '{"m_id": 4, "message": "message contents 4"}', 'PartitionKey': '4'},
                {'Data': '{"m_id": 5, "message": "message contents 5"}', 'PartitionKey': '5'},
                {'Data': '{"m_id": 6, "message": "message contents 6"}', 'PartitionKey': '6'},
                {'Data': '{"m_id": 7, "message": "message contents 7"}', 'PartitionKey': '7'},
                {'Data': '{"m_id": 8, "message": "message contents 8"}', 'PartitionKey': '8'},
                {'Data': '{"m_id": 9, "message": "message contents 9"}', 'PartitionKey': '9'},
                {'Data': '{"m_id": 10, "message": "message contents 10"}', 'PartitionKey': '10'}
            ], 'StreamName': 'test_stream'})
            # call(**{'Records': [
            #     {'Data': '{"m_id": 6, "message": "message contents 6"}', 'PartitionKey': '6'},
            #     {'Data': '{"m_id": 7, "message": "message contents 7"}', 'PartitionKey': '7'},
            #     {'Data': '{"m_id": 8, "message": "message contents 8"}', 'PartitionKey': '8'},
            #     {'Data': '{"m_id": 9, "message": "message contents 9"}', 'PartitionKey': '9'},
            #     {'Data': '{"m_id": 10, "message": "message contents 10"}', 'PartitionKey': '10'}
            # ], 'StreamName': 'test_stream'})
        ])

        # kinesis_client._individual_dispatch_method.assert_has_calls([
        #     call(**{'Data': '{"m_id": 9, "message": "message contents 9"}', 'PartitionKey': '9',
        #             'StreamName': 'test_stream'}),
        #     call(**{'Data': '{"m_id": 10, "message": "message contents 10"}', 'PartitionKey': '10',
        #             'StreamName': 'test_stream'})
        # ])

