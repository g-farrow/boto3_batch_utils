from unittest import TestCase
from unittest.mock import patch, Mock, call

from datetime import datetime

from boto3_batch_utils import CloudwatchBatchDispatcher, cloudwatch_dimension


@patch('boto3_batch_utils.Base.boto3', Mock())
class TestSqsStandard(TestCase):

    def test_more_than_one_batch_small_messages(self):
        dy_client = CloudwatchBatchDispatcher(namespace='namey_name', max_batch_size=5)

        mock_boto3 = Mock()
        dy_client._aws_service = mock_boto3
        dy_client._batch_dispatch_method = Mock()

        for i in range(0, 6):
            dy_client.submit_payload(metric_name='metricky', value=i+1, timestamp=datetime(2020, 2, 2, 1, 1, 1))

        dy_client.flush_payloads()

        self.assertEqual(2, dy_client._batch_dispatch_method.call_count)
        dy_client._batch_dispatch_method.assert_has_calls([
            call(**{'MetricData': [
                {'MetricName': 'metricky', 'Timestamp': datetime(2020, 2, 2, 1, 1, 1), 'Value': 1, 'Unit': 'Count'},
                {'MetricName': 'metricky', 'Timestamp': datetime(2020, 2, 2, 1, 1, 1), 'Value': 2, 'Unit': 'Count'},
                {'MetricName': 'metricky', 'Timestamp': datetime(2020, 2, 2, 1, 1, 1), 'Value': 3, 'Unit': 'Count'},
                {'MetricName': 'metricky', 'Timestamp': datetime(2020, 2, 2, 1, 1, 1), 'Value': 4, 'Unit': 'Count'},
                {'MetricName': 'metricky', 'Timestamp': datetime(2020, 2, 2, 1, 1, 1), 'Value': 5, 'Unit': 'Count'},
            ], 'Namespace': 'namey_name'}),
            call(**{'MetricData': [
                {'MetricName': 'metricky', 'Timestamp': datetime(2020, 2, 2, 1, 1, 1), 'Value': 6, 'Unit': 'Count'}
            ], 'Namespace': 'namey_name'})
        ])
