from unittest import TestCase
from unittest.mock import patch, Mock, call

from boto3_batch_utils.Kinesis import KinesisBatchDispatcher
from boto3_batch_utils.Base import BaseDispatcher


class MockClient:

    def __init__(self, client_name):
        self.client_name = client_name + "_client"

    def put_records(self):
        pass

    def put_record(self):
        pass


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Kinesis.DecimalEncoder')
@patch('boto3_batch_utils.Kinesis.dumps')
@patch.object(BaseDispatcher, 'submit_payload')
class SubmitPayload(TestCase):

    def test(self, mock_submit_payload, mock_json_dumps, mock_decimal_encoder):
        kn = KinesisBatchDispatcher("test_stream", partition_key_identifier="test_part_key", max_batch_size=1, flush_payload_on_max_batch_size=False)
        test_payload = {'test_part_key': 123}
        mock_json_dumps.return_value = "serialized_test_data"
        constructed_payload = {
            'Data': "serialized_test_data",
            'PartitionKey': '123'
        }
        kn.submit_payload(test_payload)
        mock_submit_payload.assert_called_once_with(constructed_payload)
        mock_json_dumps.asser_called_once_with(test_payload, cls=mock_decimal_encoder)


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, 'flush_payloads')
class FlushPayloads(TestCase):

    def test(self, mock_flush_payloads):
        kn = KinesisBatchDispatcher("test_stream", partition_key_identifier="test_part_key", max_batch_size=1, flush_payload_on_max_batch_size=False)
        kn.flush_payloads()
        mock_flush_payloads.assert_called_once_with()


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
class BatchSendPayloads(TestCase):

    def test(self):
        kn = KinesisBatchDispatcher("test_stream", partition_key_identifier="test_part_key", max_batch_size=1, flush_payload_on_max_batch_size=False)
        kn._send_single_batch_to_kinesis = Mock()
        test_batch = "a_test"
        kn._batch_send_payloads(test_batch)
        kn._send_single_batch_to_kinesis.assert_called_once_with(test_batch)


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, '_send_individual_payload')
class SendIndividualPayload(TestCase):

    def test(self, mock_send_individual_payload):
        kn = KinesisBatchDispatcher("test_stream", partition_key_identifier="test_part_key", max_batch_size=1, flush_payload_on_max_batch_size=False)
        test_payload = "test_metrics"
        kn._send_individual_payload(test_payload)
        mock_send_individual_payload.assert_called_once_with(test_payload)