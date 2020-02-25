from unittest import TestCase
from unittest.mock import patch, Mock, call

from boto3_batch_utils import SQSFifoBatchDispatcher

from .. import large_messages


@patch('boto3_batch_utils.Base.boto3', Mock())
class TestSqsStandard(TestCase):

    def test_more_than_one_batch_small_messages(self):
        sqs_client = SQSFifoBatchDispatcher(queue_name='test_standard_queue', content_based_deduplication=True)

        mock_boto3 = Mock()
        sqs_client._aws_service = mock_boto3
        mock_boto3.get_queue_url.return_value = {'QueueUrl': 'test_queue_url'}
        sqs_client._batch_dispatch_method = Mock(return_value={'hello!': True})

        test_payload = {'message': True}

        for _ in range(0, 11):
            sqs_client.submit_payload(test_payload)
        sqs_client.flush_payloads()

        self.assertEqual(2, sqs_client._batch_dispatch_method.call_count)

    def test_one_oversized_message(self):
        sqs_client = SQSFifoBatchDispatcher(queue_name='test_standard_queue', content_based_deduplication=True)

        mock_boto3 = Mock()
        sqs_client._aws_service = mock_boto3
        mock_boto3.get_queue_url.return_value = {'QueueUrl': 'test_queue_url'}
        sqs_client._batch_dispatch_method = Mock(return_value={'hello!': True})

        # An additional 92 bytes are required within the submit method
        test_payload = large_messages.create_dict_of_specific_byte_size({}, 262144 - 91)

        with self.assertRaises(ValueError) as context:
            sqs_client.submit_payload(test_payload)
        sqs_client.flush_payloads()

        self.assertIn('exceeds the maximum payload size', str(context.exception))

    def test_one_oversized_message_with_deduplication_id(self):
        sqs_client = SQSFifoBatchDispatcher(queue_name='test_standard_queue', content_based_deduplication=True)

        mock_boto3 = Mock()
        sqs_client._aws_service = mock_boto3
        mock_boto3.get_queue_url.return_value = {'QueueUrl': 'test_queue_url'}
        sqs_client._batch_dispatch_method = Mock(return_value={'hello!': True})

        # An additional 123 bytes are required within the submit method when message deup used with a value of 1 char
        test_payload = large_messages.create_dict_of_specific_byte_size({}, 262144 - 122)

        with self.assertRaises(ValueError) as context:
            sqs_client.submit_payload(test_payload, message_deduplication_id="1")
        sqs_client.flush_payloads()

        self.assertIn('exceeds the maximum payload size', str(context.exception))

    def test_more_than_one_batch_large_messages(self):
        sqs_client = SQSFifoBatchDispatcher(queue_name='test_standard_queue', content_based_deduplication=True)

        mock_boto3 = Mock()
        sqs_client._aws_service = mock_boto3
        mock_boto3.get_queue_url.return_value = {'QueueUrl': 'test_queue_url'}
        sqs_client._batch_dispatch_method = Mock(return_value={'hello!': True})

        # An additional 96 bytes are required within the submit method
        test_payload = large_messages.create_dict_of_specific_byte_size({}, 262144 - 96)

        for _ in range(0, 2):
            sqs_client.submit_payload(test_payload)
        sqs_client.flush_payloads()

        self.assertEqual(2, sqs_client._batch_dispatch_method.call_count)

    def test_batch_of_10_failed_first_time_messages(self):
        sqs_client = SQSFifoBatchDispatcher(queue_name='test_standard_queue', content_based_deduplication=True)

        mock_boto3 = Mock()
        sqs_client._aws_service = mock_boto3
        mock_boto3.get_queue_url.return_value = {'QueueUrl': 'test_queue_url'}

        failure_response = {
            'Failed': [
                {'Id': x, 'Message': 'it failed', 'SenderFault': True} for x in range(1, 11)
            ]
        }

        sqs_client._batch_dispatch_method = Mock(side_effect=[failure_response, True, True])
        sqs_client._individual_dispatch_method = Mock()

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

        for test_payload in test_payloads:
            sqs_client.submit_payload(test_payload, message_id=test_payload['m_id'])
        sqs_client.flush_payloads()

        sqs_client._batch_dispatch_method.assert_called_once()
        sqs_client._individual_dispatch_method.assert_has_calls([
            call(**{'Id': 1, 'MessageBody': '{"m_id": 1, "message": "message contents 1"}',
                    'MessageGroupId': 'unset', 'QueueUrl': 'test_queue_url'}),
            call(**{'Id': 2, 'MessageBody': '{"m_id": 2, "message": "message contents 2"}',
                    'MessageGroupId': 'unset', 'QueueUrl': 'test_queue_url'}),
            call(**{'Id': 3, 'MessageBody': '{"m_id": 3, "message": "message contents 3"}',
                    'MessageGroupId': 'unset', 'QueueUrl': 'test_queue_url'}),
            call(**{'Id': 4, 'MessageBody': '{"m_id": 4, "message": "message contents 4"}',
                    'MessageGroupId': 'unset', 'QueueUrl': 'test_queue_url'}),
            call(**{'Id': 5, 'MessageBody': '{"m_id": 5, "message": "message contents 5"}',
                    'MessageGroupId': 'unset', 'QueueUrl': 'test_queue_url'}),
            call(**{'Id': 6, 'MessageBody': '{"m_id": 6, "message": "message contents 6"}',
                    'MessageGroupId': 'unset', 'QueueUrl': 'test_queue_url'}),
            call(**{'Id': 7, 'MessageBody': '{"m_id": 7, "message": "message contents 7"}',
                    'MessageGroupId': 'unset', 'QueueUrl': 'test_queue_url'}),
            call(**{'Id': 8, 'MessageBody': '{"m_id": 8, "message": "message contents 8"}',
                    'MessageGroupId': 'unset', 'QueueUrl': 'test_queue_url'}),
            call(**{'Id': 9, 'MessageBody': '{"m_id": 9, "message": "message contents 9"}',
                    'MessageGroupId': 'unset', 'QueueUrl': 'test_queue_url'}),
            call(**{'Id': 10, 'MessageBody': '{"m_id": 10, "message": "message contents 10"}',
                    'MessageGroupId': 'unset', 'QueueUrl': 'test_queue_url'})
        ], any_order=True)
