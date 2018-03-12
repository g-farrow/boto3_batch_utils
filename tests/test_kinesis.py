from unittest import TestCase
from unittest.mock import patch, Mock, call

from json import dumps
from base64 import standard_b64encode

from boto3_batch_utils.Kinesis import KinesisBatchDispatcher
from boto3_batch_utils.Base import BaseDispatcher


class MockClient:

    def __init__(self, client_name):
        self.client_name = client_name + "_client"

    def put_records(self, Records=[], StreamName='string'):
        pass

    def put_record(self, StreamName='string', Data=b'bytes', PartitionKey='string'):
        pass


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Kinesis.DecimalEncoder')
@patch('boto3_batch_utils.Kinesis.dumps')
@patch.object(BaseDispatcher, 'submit_payload')
class SubmitPayload(TestCase):

    def test(self, mock_submit_payload, mock_json_dumps, mock_decimal_encoder):
        kn = KinesisBatchDispatcher("test_stream", partition_key_identifier="test_part_key", max_batch_size=1,
                                    flush_payload_on_max_batch_size=False)
        test_payload = {'test_part_key': 123}
        mock_json_dumps.return_value = "serialized_test_data"
        constructed_payload = {
            'Data': standard_b64encode(str.encode("serialized_test_data")),
            'PartitionKey': '123'
        }
        kn.submit_payload(test_payload)
        mock_submit_payload.assert_called_once_with(constructed_payload)
        mock_json_dumps.asser_called_once_with(test_payload, cls=mock_decimal_encoder)


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, 'flush_payloads')
class FlushPayloads(TestCase):

    def test(self, mock_flush_payloads):
        kn = KinesisBatchDispatcher("test_stream", partition_key_identifier="test_part_key", max_batch_size=1,
                                    flush_payload_on_max_batch_size=False)
        kn.flush_payloads()
        mock_flush_payloads.assert_called_once_with()


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, '_batch_send_payloads')
class BatchSendPayloads(TestCase):

    def test(self, mock_base_batch_send_payloads):
        kn = KinesisBatchDispatcher("test_stream", partition_key_identifier="test_part_key", max_batch_size=1,
                                    flush_payload_on_max_batch_size=False)
        test_batch = "a_test"
        kn._batch_send_payloads(test_batch)
        mock_base_batch_send_payloads.assert_called_once_with({'StreamName': 'test_stream', 'Records': test_batch})


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
class ProcessFailedPayloads(TestCase):

    def test_all_records_failed_in_first_batch_and_are_re_submitted(self):
        kn = KinesisBatchDispatcher("test_stream", partition_key_identifier="test_part_key", max_batch_size=1,
                                    flush_payload_on_max_batch_size=False)
        kn._batch_send_payloads = Mock()
        test_batch = [
            {"Id": 1}, {"Id": 2}, {"Id": 3}, {"Id": 4}, {"Id": 5},
            {"Id": 6}, {"Id": 7}, {"Id": 8}, {"Id": 9}, {"Id": 10}
        ]
        kn.batch_in_progress = test_batch
        test_response = {
            'FailedRecordCount': 10,
            'Records': [
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")}
            ]
        }
        kn._process_failed_payloads(test_response)
        kn._batch_send_payloads.assert_called_once_with(test_batch)

    def test_some_records_are_rejected_some_are_successful(self):
        kn = KinesisBatchDispatcher("test_stream", partition_key_identifier="test_part_key", max_batch_size=1,
                                    flush_payload_on_max_batch_size=False)
        kn._batch_send_payloads = Mock()
        test_batch = [
            {"Id": 1}, {"Id": 2}, {"Id": 3}, {"Id": 4}, {"Id": 5},
            {"Id": 6}, {"Id": 7}, {"Id": 8}, {"Id": 9}, {"Id": 10}
        ]
        kn.batch_in_progress = test_batch
        test_response = {
            'FailedRecordCount': 5,
            'Records': [
                {'SequenceNumber': '49580022882545286363048362619667912448714664261560827906',
                 'ShardId': 'shardId-000000000000'},
                {'SequenceNumber': '49580022882545286363048362619669121374534278890735534082',
                 'ShardId': 'shardId-000000000000'},
                {'SequenceNumber': '49580022882545286363048362619670330300353893519910240258',
                 'ShardId': 'shardId-000000000000'},
                {'SequenceNumber': '49580022882545286363048362619671539226173508149084946434',
                 'ShardId': 'shardId-000000000000'},
                {'SequenceNumber': '49580022882545286363048362619673957077812737407434358786',
                 'ShardId': 'shardId-000000000000'},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")}
            ]
        }
        kn._process_failed_payloads(test_response)
        kn._batch_send_payloads.assert_called_once_with([{"Id": 6}, {"Id": 7}, {"Id": 8}, {"Id": 9}, {"Id": 10}])

    def test_two_records_are_rejected_the_rest_are_successful(self):
        kn = KinesisBatchDispatcher("test_stream", partition_key_identifier="test_part_key", max_batch_size=1,
                                    flush_payload_on_max_batch_size=False)
        kn.individual_dispatch_method = Mock()
        test_batch = [
            {'Data': standard_b64encode(str.encode(dumps({"Id": 1}))), 'PartitionKey': 'Id'},
            {'Data': standard_b64encode(str.encode(dumps({"Id": 2}))), 'PartitionKey': 'Id'},
            {'Data': standard_b64encode(str.encode(dumps({"Id": 3}))), 'PartitionKey': 'Id'},
            {'Data': standard_b64encode(str.encode(dumps({"Id": 4}))), 'PartitionKey': 'Id'},
            {'Data': standard_b64encode(str.encode(dumps({"Id": 5}))), 'PartitionKey': 'Id'},
            {'Data': standard_b64encode(str.encode(dumps({"Id": 6}))), 'PartitionKey': 'Id'},
            {'Data': standard_b64encode(str.encode(dumps({"Id": 7}))), 'PartitionKey': 'Id'}
        ]
        kn.batch_in_progress = test_batch
        test_response = {
            'FailedRecordCount': 2,
            'Records': [
                {'SequenceNumber': '49580022882545286363048362619667912448714664261560827906',
                 'ShardId': 'shardId-000000000000'},
                {'SequenceNumber': '49580022882545286363048362619669121374534278890735534082',
                 'ShardId': 'shardId-000000000000'},
                {'SequenceNumber': '49580022882545286363048362619670330300353893519910240258',
                 'ShardId': 'shardId-000000000000'},
                {'SequenceNumber': '49580022882545286363048362619671539226173508149084946434',
                 'ShardId': 'shardId-000000000000'},
                {'SequenceNumber': '49580022882545286363048362619673957077812737407434358786',
                 'ShardId': 'shardId-000000000000'},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")},
                {'ErrorCode': 'ProvisionedThroughputExceededException',
                 'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                 ' under account {}.'.format("test_stream", "aws_account_id")}
            ]
        }
        kn._process_failed_payloads(test_response)
        kn.individual_dispatch_method.assert_has_calls([
            call(**{'StreamName': 'test_stream', 'Data': standard_b64encode(str.encode(dumps({"Id": 6}))), 'PartitionKey': 'Id'}),
            call(**{'StreamName': 'test_stream', 'Data': standard_b64encode(str.encode(dumps({"Id": 7}))), 'PartitionKey': 'Id'})
        ])


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
class ProcessBatchSendResponse(TestCase):

    def test_no_records_attribute_in_response(self):
        kn = KinesisBatchDispatcher("test_stream", partition_key_identifier="test_part_key", max_batch_size=1,
                                    flush_payload_on_max_batch_size=False)
        kn._process_failed_payloads = Mock()
        test_batch = [
            {"Id": 1}
        ]
        test_response = {
            'FailedRecordCount': 0,
            'Records': [1],
            'EncryptionType': 'KMS'
        }
        kn.batch_in_progress = test_batch
        kn._process_batch_send_response(test_response)
        kn._process_failed_payloads.assert_not_called()

    def test_no_failed_records_in_response(self):
        kn = KinesisBatchDispatcher("test_stream", partition_key_identifier="test_part_key", max_batch_size=1,
                                    flush_payload_on_max_batch_size=False)
        kn._process_failed_payloads = Mock()
        test_batch = [
            {"Id": 1}, {"Id": 2}, {"Id": 3}, {"Id": 4}, {"Id": 5},
            {"Id": 6}, {"Id": 7}, {"Id": 8}, {"Id": 9}
        ]
        test_response = {
            'FailedRecordCount': 0,
            'Records': [1, 2, 3, 4, 5, 6, 7, 8, 9],
            'EncryptionType': 'KMS'
        }
        kn.batch_in_progress = test_batch
        kn._process_batch_send_response(test_response)
        kn._process_failed_payloads.assert_not_called()

    def test_all_records_failed(self):
        kn = KinesisBatchDispatcher("test_stream", partition_key_identifier="test_part_key", max_batch_size=1,
                                    flush_payload_on_max_batch_size=False)
        kn._process_failed_payloads = Mock()
        test_batch = [
            {"Id": 1}, {"Id": 2}, {"Id": 3}, {"Id": 4}, {"Id": 5},
            {"Id": 6}, {"Id": 7}, {"Id": 8}, {"Id": 9}, {"Id": 10}
        ]
        test_response = {
            'FailedRecordCount': 10,
            'Records': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        }
        kn.batch_in_progress = test_batch
        kn._process_batch_send_response(test_response)
        kn._process_failed_payloads.assert_called_once_with(test_response)

    def test_some_records_failed(self):
        kn = KinesisBatchDispatcher("test_stream", partition_key_identifier="test_part_key", max_batch_size=1,
                                    flush_payload_on_max_batch_size=False)
        kn._process_failed_payloads = Mock()
        test_batch = [
            {"Id": 1}, {"Id": 2}, {"Id": 3}, {"Id": 4}, {"Id": 5},
            {"Id": 6}, {"Id": 7}, {"Id": 8}, {"Id": 9}, {"Id": 10}
        ]
        test_response = {
            'FailedRecordCount': 5,
            'Records': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        }
        kn.batch_in_progress = test_batch
        kn._process_batch_send_response(test_response)
        kn._process_failed_payloads.assert_called_once_with(test_response)



@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch.object(BaseDispatcher, '_send_individual_payload')
class SendIndividualPayload(TestCase):

    def test(self, mock_send_individual_payload):
        kn = KinesisBatchDispatcher("test_stream", partition_key_identifier="test_part_key", max_batch_size=1,
                                    flush_payload_on_max_batch_size=False)
        test_payload = {
            'Data': "{'something': 'else'}",
            'PartitionKey': 'Id'
        }
        kn._send_individual_payload(test_payload)
        _test_payload = test_payload
        _test_payload['StreamName'] = 'test_stream'
        mock_send_individual_payload.assert_called_once_with(_test_payload)
