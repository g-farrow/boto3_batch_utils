from unittest import TestCase
from unittest.mock import patch

from boto3_batch_utils.Dynamodb import DynamoBatchDispatcher
from boto3_batch_utils.Base import BaseDispatcher


class MockClient:

    def __init__(self, client_name):
        self.client_name = client_name + "_client"

    def batch_write_item(self):
        pass

    def put_item(self):
        pass


# @patch('boto3_batch_utils.Base.boto3.client', MockClient)
# @patch.object(BaseDispatcher, 'submit_payload')
# class SubmitPayload(TestCase):
#
#     def test(self, mock_submit_payload):
#         dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
#         test_payload = {}
#         dy.submit_payload()
#         mock_submit_payload.assert_called_once_with()
#
#
# @patch('boto3_batch_utils.Base.boto3.client', MockClient)
# @patch.object(BaseDispatcher, 'flush_payloads')
# class FlushPayloads(TestCase):
#
#     def test(self, mock_flush_payloads):
#         dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
#         dy.flush_payloads()
#         mock_flush_payloads.assert_called_once_with()
#
#
# @patch('boto3_batch_utils.Base.boto3.client', MockClient)
# @patch.object(BaseDispatcher, '_batch_send_payloads')
# class BatchSendPayloads(TestCase):
#
#     def test(self, mock_batch_send_payloads):
#         dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
#         test_batch = "a_test"
#         dy._batch_send_payloads(test_batch)
#         mock_batch_send_payloads.assert_called_once_with(test_batch)
#
#
# @patch('boto3_batch_utils.Base.boto3.client', MockClient)
# @patch.object(BaseDispatcher, '_send_individual_payload')
# class SendIndividualPayload(TestCase):
#
#     def test(self, mock_send_individual_payload):
#         dy = DynamoBatchDispatcher('test_table_name', 'p_key', max_batch_size=1, flush_payload_on_max_batch_size=False)
#         test_metric = "test_metrics"
#         dy._send_individual_payload(test_metric)
#         mock_send_individual_payload.assert_called_once_with(test_metric)
