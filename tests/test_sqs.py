from unittest import TestCase
from unittest.mock import patch, Mock, call

from boto3_batch_utils.SQS import SQSBatchDispatcher
from boto3_batch_utils.Base import BaseDispatcher


class MockClient:

    def __init__(self, client_name):
        self.client_name = client_name + "_client"

    def send_message_batch(self):
        pass

    def send_message(self):
        pass

    def get_queue_url(self, **kwargs):
        return {"QueueUrl": "test_url"}


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, 'submit_payload')
class SubmitPayload(TestCase):

    def test(self, mock_submit_payload):
        sqs = SQSBatchDispatcher('test_queue', max_batch_size=1, flush_payload_on_max_batch_size=False)
        test_message = {'something': 'else'}
        test_id = 123
        test_delay = 3
        sqs.submit_payload(test_message, test_id, test_delay)
        mock_submit_payload.assert_called_once_with(
            {'Id': test_id, 'MessageBody': test_message, 'DelaySeconds': test_delay}
        )


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, 'flush_payloads')
class FlushPayloads(TestCase):

    def test(self, mock_flush_payloads):
        sqs = SQSBatchDispatcher('test_queue', max_batch_size=1, flush_payload_on_max_batch_size=False)
        sqs.flush_payloads()
        mock_flush_payloads.assert_called_once_with()


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, '_batch_send_payloads')
class BatchSendPayloads(TestCase):

    def test(self, mock_batch_send_payloads):
        sqs = SQSBatchDispatcher('test_queue', max_batch_size=1, flush_payload_on_max_batch_size=False)
        test_batch = "a_test"
        sqs._batch_send_payloads(test_batch)
        mock_batch_send_payloads.assert_called_once_with(**{'QueueUrl': "test_url", 'Entries': test_batch})


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
class ProcessFailedPayloads(TestCase):

    def test_all_records_failed_in_first_batch_and_are_re_submitted(self):
        sqs = SQSBatchDispatcher('test_queue', max_batch_size=1, flush_payload_on_max_batch_size=False)
        sqs._send_individual_payload = Mock()
        test_batch = [
            {'Id': '1', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '2', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '3', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '4', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '5', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '6', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '7', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '8', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '9', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '10', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}
        ]
        sqs.batch_in_progress = test_batch
        test_response = {
            'Successful': [],
            'Failed': [
                {'Id': '1', 'SenderFault': True, 'Code': 'ABCD', 'Message': "Something bad happened here"},
                {'Id': '2', 'SenderFault': True, 'Code': 'ABCD', 'Message': "Something bad happened here"},
                {'Id': '3', 'SenderFault': True, 'Code': 'ABCD', 'Message': "Something bad happened here"},
                {'Id': '4', 'SenderFault': True, 'Code': 'ABCD', 'Message': "Something bad happened here"},
                {'Id': '5', 'SenderFault': True, 'Code': 'ABCD', 'Message': "Something bad happened here"},
                {'Id': '6', 'SenderFault': True, 'Code': 'ABCD', 'Message': "Something bad happened here"},
                {'Id': '7', 'SenderFault': True, 'Code': 'ABCD', 'Message': "Something bad happened here"},
                {'Id': '8', 'SenderFault': True, 'Code': 'ABCD', 'Message': "Something bad happened here"},
                {'Id': '9', 'SenderFault': True, 'Code': 'ABCD', 'Message': "Something bad happened here"},
                {'Id': '10', 'SenderFault': True, 'Code': 'ABCD', 'Message': "Something bad happened here"},
            ]
        }
        sqs._process_batch_send_response(test_response)
        sqs._send_individual_payload.assert_has_calls([
            call({'Id': '1', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}),
            call({'Id': '2', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}),
            call({'Id': '3', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}),
            call({'Id': '4', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}),
            call({'Id': '5', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}),
            call({'Id': '6', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}),
            call({'Id': '7', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}),
            call({'Id': '8', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}),
            call({'Id': '9', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}),
            call({'Id': '10', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}),
        ])

    def test_some_records_are_rejected_some_are_successful(self):
        sqs = SQSBatchDispatcher('test_queue', max_batch_size=1, flush_payload_on_max_batch_size=False)
        sqs._send_individual_payload = Mock()
        test_batch = [
            {'Id': '1', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '2', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '3', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '4', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '5', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '6', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '7', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '8', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '9', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7},
            {'Id': '10', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}
        ]
        sqs.batch_in_progress = test_batch
        test_response = {
            'Successful': [
                {'Id': '1', 'MessageId': '', 'MD5OfMessageBody': '', 'MD5OfMessageAttributes': '', 'SequenceNumber': ''},
                {'Id': '2', 'MessageId': '', 'MD5OfMessageBody': '', 'MD5OfMessageAttributes': '', 'SequenceNumber': ''},
                {'Id': '3', 'MessageId': '', 'MD5OfMessageBody': '', 'MD5OfMessageAttributes': '', 'SequenceNumber': ''},
                {'Id': '4', 'MessageId': '', 'MD5OfMessageBody': '', 'MD5OfMessageAttributes': '', 'SequenceNumber': ''},
                {'Id': '5', 'MessageId': '', 'MD5OfMessageBody': '', 'MD5OfMessageAttributes': '', 'SequenceNumber': ''},
            ],
            'Failed': [
                {'Id': '6', 'SenderFault': True, 'Code': 'ABCD', 'Message': "Something bad happened here"},
                {'Id': '7', 'SenderFault': True, 'Code': 'ABCD', 'Message': "Something bad happened here"},
                {'Id': '8', 'SenderFault': True, 'Code': 'ABCD', 'Message': "Something bad happened here"},
                {'Id': '9', 'SenderFault': True, 'Code': 'ABCD', 'Message': "Something bad happened here"},
                {'Id': '10', 'SenderFault': True, 'Code': 'ABCD', 'Message': "Something bad happened here"},
            ]
        }
        sqs._process_batch_send_response(test_response)
        sqs._send_individual_payload.assert_has_calls([
            call({'Id': '6', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}),
            call({'Id': '7', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}),
            call({'Id': '8', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}),
            call({'Id': '9', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}),
            call({'Id': '10', 'MessageBody': {'something_to_send': 'etc'}, 'DelaySeconds': 7}),
        ])


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, '_send_individual_payload')
class SendIndividualPayload(TestCase):

    def test(self, mock_send_individual_payload):
        sqs = SQSBatchDispatcher('test_queue', max_batch_size=1, flush_payload_on_max_batch_size=False)
        test_payload = {
            'Id': 12345,
            'MessageBody': "some_sort_of_payload",
            'DelaySeconds': 99
            }
        sqs._send_individual_payload(test_payload)
        expected_converted_payload = {"QueueUrl": "test_url", "MessageBody": "some_sort_of_payload",
                                      "DelaySeconds": 99}
        mock_send_individual_payload.assert_called_once_with(expected_converted_payload, retry=4)

