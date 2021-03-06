from unittest import TestCase
from unittest.mock import patch, Mock
from datetime import datetime

from boto3_batch_utils.Cloudwatch import CloudwatchBatchDispatcher, cloudwatch_dimension
from boto3_batch_utils.Base import BaseDispatcher


class MockClient:

    def __init__(self, client_name):
        self.client_name = client_name + "_client"

    def put_metric_data(self):
        pass


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
@patch.object(BaseDispatcher, 'submit_payload')
class SubmitMetric(TestCase):

    def test(self, mock_submit_payload):
        cw = CloudwatchBatchDispatcher('test_space', max_batch_size=1)
        mock_metric_name = 'met'
        mock_timestamp = datetime.now()
        mock_dimensions = None
        mock_value = 123
        mock_unit = 'Bytes'
        cw.submit_metric(metric_name=mock_metric_name, value=mock_value, timestamp=mock_timestamp,
                         dimensions=mock_dimensions, unit=mock_unit)
        mock_submit_payload.assert_called_once_with({
            'MetricName': mock_metric_name,
            'Timestamp': mock_timestamp,
            'Value': mock_value,
            'Unit': mock_unit
        })


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
@patch.object(BaseDispatcher, 'flush_payloads')
class FlushPayloads(TestCase):

    def test(self, mock_flush_payloads):
        cw = CloudwatchBatchDispatcher('test_space', max_batch_size=1)
        cw.flush_payloads()
        mock_flush_payloads.assert_called_once_with()


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
@patch.object(BaseDispatcher, '_batch_send_payloads')
class BatchSendPayloads(TestCase):

    def test(self, mock_batch_send_payloads):
        cw = CloudwatchBatchDispatcher('test_space', max_batch_size=1)
        test_batch = {'test': True}
        cw._batch_send_payloads(test_batch)
        mock_batch_send_payloads.assert_called_once_with({'Namespace': 'test_space', 'MetricData': test_batch})


class CloudwatchDimensionStructure(TestCase):

    def test(self):
        self.assertEqual(
            {'Name': "test_name", 'Value': '123'},
            cloudwatch_dimension("test_name", 123)
        )
