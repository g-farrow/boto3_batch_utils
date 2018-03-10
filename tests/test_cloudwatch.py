from unittest import TestCase
from unittest.mock import patch
from datetime import datetime

from boto3_batch_utils.Cloudwatch import CloudwatchBatchDispatcher, cloudwatch_dimension
from boto3_batch_utils.Base import BaseDispatcher


class MockClient:

    def __init__(self, client_name):
        self.client_name = client_name + "_client"

    def put_metrics(self):
        pass

    def put_metric(self):
        pass


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, 'submit_payload')
class SubmitPayload(TestCase):

    def test(self, mock_submit_payload):
        cw = CloudwatchBatchDispatcher('test_space', max_batch_size=1, flush_payload_on_max_batch_size=False)
        mock_metric_name = None
        mock_timestamp = datetime.now()
        mock_dimensions = None
        mock_value = None
        mock_unit = None
        cw.submit_payload(mock_metric_name, mock_timestamp, mock_dimensions, mock_value, mock_unit)
        mock_submit_payload.assert_called_once_with({
            'MetricName': mock_metric_name,
            'Dimensions': mock_dimensions,
            'Timestamp': mock_timestamp,
            'Value': mock_value,
            'Unit': mock_unit
        })


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, 'flush_payloads')
class FlushPayloads(TestCase):

    def test(self, mock_flush_payloads):
        cw = CloudwatchBatchDispatcher('test_space', max_batch_size=1, flush_payload_on_max_batch_size=False)
        cw.flush_payloads()
        mock_flush_payloads.assert_called_once_with()


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, '_batch_send_payloads')
class BatchSendPayloads(TestCase):

    def test(self, mock_batch_send_payloads):
        cw = CloudwatchBatchDispatcher('test_space', max_batch_size=1, flush_payload_on_max_batch_size=False)
        test_batch = "a_test"
        cw._batch_send_payloads(test_batch)
        mock_batch_send_payloads.assert_called_once_with(test_batch)


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, '_send_individual_payload')
class SendIndividualPayload(TestCase):

    def test(self, mock_send_individual_payload):
        cw = CloudwatchBatchDispatcher('test_space', max_batch_size=1, flush_payload_on_max_batch_size=False)
        test_metric = "test_metrics"
        cw._send_individual_payload(test_metric)
        mock_send_individual_payload.assert_called_once_with(test_metric)


class CloudwatchDimensionStructure(TestCase):

    def test(self):
        self.assertEqual(
            {'Name': "test_name", 'Value': 123},
            cloudwatch_dimension("test_name", 123)
        )