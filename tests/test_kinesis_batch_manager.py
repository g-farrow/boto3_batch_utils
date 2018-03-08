from boto3_batch_utils import kinesis_batch_manager
import unittest
from unittest.mock import Mock, patch, call
import logging
from json import dumps

logger = logging.getLogger()


test_message = {'Id': 'abc1', 'message': 'text'}


class Chunks(unittest.TestCase):
    """
    Pass an array and a desired batch size, and have the function split the array into a serious of batches which are
    less than or equal to the desired batch size
    """

    def test_1_record_batch_size_1(self):
        """
        The batch list contains 1 batch, with 1 item in the batch
        """
        array = ['a']
        batch_size = 1
        km = kinesis_batch_manager.KinesisBatchPutManager('TEST_STREAM', '', batch_size)
        km.records_to_send = array
        batch_list = km._split_records_to_send_into_batches()
        self.assertEqual(len(batch_list), 1, msg="Batch list length")
        self.assertEqual(len(batch_list[0]), 1, msg="Size of first batch")
        self.assertEqual(batch_list[0][0], 'a', msg="Content of the first batch")

    def test_2_records_batch_size_1(self):
        """
        The batch list contains 2 batches, with 1 item in each
        """
        array = ['a', 'b']
        batch_size = 1
        km = kinesis_batch_manager.KinesisBatchPutManager('TEST_STREAM', '', batch_size)
        km.records_to_send = array
        batch_list = km._split_records_to_send_into_batches()
        self.assertEqual(len(batch_list), 2, msg="Batch list length")
        self.assertEqual(len(batch_list[0]), 1, msg="Size of first batch")
        self.assertEqual(batch_list[0][0], 'a', msg="Content of the first batch")
        self.assertEqual(len(batch_list[1]), 1, msg="Size of second batch")
        self.assertEqual(batch_list[1][0], 'b', msg="Content of the second batch")

    def test_2_records_batch_size_2(self):
        """
        The batch list contains 1 batch, with 2 items
        """
        array = ['a', 'b']
        batch_size = 2
        km = kinesis_batch_manager.KinesisBatchPutManager('TEST_STREAM', '', batch_size)
        km.records_to_send = array
        batch_list = km._split_records_to_send_into_batches()
        self.assertEqual(len(batch_list), 1, msg="Batch list length")
        self.assertEqual(len(batch_list[0]), 2, msg="Size of first batch")
        self.assertEqual(batch_list[0], ['a', 'b'], msg="Content of the first batch")

    def test_20_records_batch_size_7(self):
        """
        The batch list contains 3 batches, with 7 items, 7 items, 6 items respectively
        """
        array = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        batch_size = 7
        km = kinesis_batch_manager.KinesisBatchPutManager('TEST_STREAM', '', batch_size)
        km.records_to_send = array
        batch_list = km._split_records_to_send_into_batches()
        self.assertEqual(len(batch_list), 3, msg="Batch list length")
        self.assertEqual(len(batch_list[0]), 7, msg="Size of first batch")
        self.assertEqual(len(batch_list[1]), 7, msg="Size of second batch")
        self.assertEqual(len(batch_list[2]), 6, msg="Size of third batch")
        self.assertEqual(batch_list[0], [1, 2, 3, 4, 5, 6, 7], msg="Content of the first batch")
        self.assertEqual(batch_list[1], [8, 9, 10, 11, 12, 13, 14], msg="Content of the second batch")
        self.assertEqual(batch_list[2], [15, 16, 17, 18, 19, 20], msg="Content of the third batch")


@patch('boto3_batch_utils.kinesis_batch_manager.boto3', Mock())
@patch('boto3_batch_utils.kinesis_batch_manager.boto3.client', Mock(return_value=Mock()))
class SendBatchToKinesis(unittest.TestCase):
    """
    Batch send records to Kinesis. Handle any records which failed to be put in the first attempt, by retrying them.
    """

    def test_one_record_all_successful(self):
        """
        Send a batch and ensure that all records in the batch are eventually successfully delivered to Kinesis
        """
        kinesis_stream_name = 'TEST_STREAM'
        test_batch = [{'Data': '{}'.format(test_message), 'PartitionKey': 'tbd'}]
        put_records_return_value = {
            'FailedRecordCount': 0,
            'Records': [
                {'SequenceNumber': '49580022882545286363048362619666703522895049632386121730',
                 'ShardId': 'shardId-000000000000'},
            ],
            'EncryptionType': 'KMS'
        }
        km = kinesis_batch_manager.KinesisBatchPutManager(kinesis_stream_name, '', 0)
        km.kinesis_client.put_records = Mock(return_value=put_records_return_value)
        km._send_single_batch_to_kinesis(test_batch)
        km.kinesis_client.put_records.assert_called_once_with(StreamName=kinesis_stream_name, Records=test_batch)

    def test_lots_of_records_all_successful(self):
        """
        Send a batch and ensure that all records in the batch are eventually successfully delivered to Kinesis
        """
        kinesis_stream_name = 'TEST_STREAM'
        test_batch = [
            {"Id": 1}, {"Id": 2}, {"Id": 3}, {"Id": 4}, {"Id": 5},
            {"Id": 6}, {"Id": 7}, {"Id": 8}, {"Id": 9}
        ]
        put_records_return_value = {
                'FailedRecordCount': 0,
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
                    {'SequenceNumber': '49580022882545286363048362619675166003632352036609064962',
                     'ShardId': 'shardId-000000000000'},
                    {'SequenceNumber': '49580022882545286363048362619676374929451966665783771138',
                     'ShardId': 'shardId-000000000000'},
                    {'SequenceNumber': '49580022882545286363048362619677583855271581294958477314',
                     'ShardId': 'shardId-000000000000'},
                    {'SequenceNumber': '49580022882545286363048362619678792781091195924133183490',
                     'ShardId': 'shardId-000000000000'}
                ],
                'EncryptionType': 'KMS'
            }
        km = kinesis_batch_manager.KinesisBatchPutManager(kinesis_stream_name, '', 0)
        km.kinesis_client.put_records = Mock(return_value=put_records_return_value)
        km._send_single_batch_to_kinesis(test_batch)
        km.kinesis_client.put_records.assert_called_once_with(StreamName=kinesis_stream_name, Records=test_batch)

    def test_lots_of_records_none_successful_then_all_successful(self):
        """
        Send a batch and ensure that all records in the batch are eventually successfully delivered to Kinesis
        """
        kinesis_stream_name = 'TEST_STREAM'
        test_batch = [
            {"Id": 1}, {"Id": 2}, {"Id": 3}, {"Id": 4}, {"Id": 5},
            {"Id": 6}, {"Id": 7}, {"Id": 8}, {"Id": 9}, {"Id": 10}
        ]
        put_records_side_effect = [
            {
                'FailedRecordCount': 10,
                'Records': [
                    {'ErrorCode': 'ProvisionedThroughputExceededException',
                     'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                     ' under account {}.'.format(kinesis_stream_name, "aws_account_id")},
                    {'ErrorCode': 'ProvisionedThroughputExceededException',
                     'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                     ' under account {}.'.format(kinesis_stream_name, "aws_account_id")},
                    {'ErrorCode': 'ProvisionedThroughputExceededException',
                     'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                     ' under account {}.'.format(kinesis_stream_name, "aws_account_id")},
                    {'ErrorCode': 'ProvisionedThroughputExceededException',
                     'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                     ' under account {}.'.format(kinesis_stream_name, "aws_account_id")},
                    {'ErrorCode': 'ProvisionedThroughputExceededException',
                     'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                     ' under account {}.'.format(kinesis_stream_name, "aws_account_id")},
                    {'ErrorCode': 'ProvisionedThroughputExceededException',
                     'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                     ' under account {}.'.format(kinesis_stream_name, "aws_account_id")},
                    {'ErrorCode': 'ProvisionedThroughputExceededException',
                     'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                     ' under account {}.'.format(kinesis_stream_name, "aws_account_id")},
                    {'ErrorCode': 'ProvisionedThroughputExceededException',
                     'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                     ' under account {}.'.format(kinesis_stream_name, "aws_account_id")},
                    {'ErrorCode': 'ProvisionedThroughputExceededException',
                     'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                     ' under account {}.'.format(kinesis_stream_name, "aws_account_id")},
                    {'ErrorCode': 'ProvisionedThroughputExceededException',
                     'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                     ' under account {}.'.format(kinesis_stream_name, "aws_account_id")}
                ]
            },
            {
                'FailedRecordCount': 0,
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
                    {'SequenceNumber': '49580022882545286363048362619675166003632352036609064962',
                     'ShardId': 'shardId-000000000000'},
                    {'SequenceNumber': '49580022882545286363048362619676374929451966665783771138',
                     'ShardId': 'shardId-000000000000'},
                    {'SequenceNumber': '49580022882545286363048362619677583855271581294958477314',
                     'ShardId': 'shardId-000000000000'},
                    {'SequenceNumber': '49580022882545286363048362619678792781091195924133183490',
                     'ShardId': 'shardId-000000000000'},
                    {'SequenceNumber': '49580022882545286363048362619666703522895049632386121730',
                     'ShardId': 'shardId-000000000000'}
                ],
                'EncryptionType': 'KMS'
            }
        ]
        km = kinesis_batch_manager.KinesisBatchPutManager(kinesis_stream_name, '', 0)
        km.kinesis_client.put_records = Mock(return_value=put_records_side_effect)
        km._send_single_batch_to_kinesis(test_batch)
        km.kinesis_client.put_records.has_calls(
            call(StreamName=kinesis_stream_name, Records=test_batch),
            call(StreamName=kinesis_stream_name, Records=test_batch)
        )

    def test_lots_of_records_some_successful_then_the_rest_successful(self):
        """
        Send a batch and ensure that all records in the batch are eventually successfully delivered to Kinesis
        """
        kinesis_stream_name = 'TEST_STREAM'
        test_batch = [
            {"Id": 1}, {"Id": 2}, {"Id": 3}, {"Id": 4}, {"Id": 5},
            {"Id": 6}, {"Id": 7}, {"Id": 8}, {"Id": 9}, {"Id": 10}
        ]
        put_records_side_effect = [
            {
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
                                     ' under account {}.'.format(kinesis_stream_name, "aws_account_id")},
                    {'ErrorCode': 'ProvisionedThroughputExceededException',
                     'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                     ' under account {}.'.format(kinesis_stream_name, "aws_account_id")},
                    {'ErrorCode': 'ProvisionedThroughputExceededException',
                     'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                     ' under account {}.'.format(kinesis_stream_name, "aws_account_id")},
                    {'ErrorCode': 'ProvisionedThroughputExceededException',
                     'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                     ' under account {}.'.format(kinesis_stream_name, "aws_account_id")},
                    {'ErrorCode': 'ProvisionedThroughputExceededException',
                     'ErrorMessage': 'Rate exceeded for shard shardId-000000000000 in stream {}'
                                     ' under account {}.'.format(kinesis_stream_name, "aws_account_id")}
                ]
            },
            {
                'FailedRecordCount': 0,
                'Records': [
                    {'SequenceNumber': '49580022882545286363048362619675166003632352036609064962',
                     'ShardId': 'shardId-000000000000'},
                    {'SequenceNumber': '49580022882545286363048362619676374929451966665783771138',
                     'ShardId': 'shardId-000000000000'},
                    {'SequenceNumber': '49580022882545286363048362619677583855271581294958477314',
                     'ShardId': 'shardId-000000000000'},
                    {'SequenceNumber': '49580022882545286363048362619678792781091195924133183490',
                     'ShardId': 'shardId-000000000000'},
                    {'SequenceNumber': '49580022882545286363048362619666703522895049632386121730',
                     'ShardId': 'shardId-000000000000'}
                ],
                'EncryptionType': 'KMS'
            }
        ]
        km = kinesis_batch_manager.KinesisBatchPutManager(kinesis_stream_name, '', 0)
        km.kinesis_client.put_records = Mock(return_value=put_records_side_effect)
        km._send_single_batch_to_kinesis(test_batch)
        km.kinesis_client.put_records.has_calls(
            call(StreamName=kinesis_stream_name, Records=test_batch),
            call(StreamName=kinesis_stream_name, Records=test_batch[5:])
        )


@patch('boto3_batch_utils.kinesis_batch_manager.boto3', Mock())
@patch('boto3_batch_utils.kinesis_batch_manager.boto3.client', Mock(return_value=Mock()))
class SubmitRecord(unittest.TestCase):
    """
    When a new record is submitted, it should be properly formatted
    """

    def test(self):
        test_record = {'record_key': {'an id': 'hey there!'}, 'like': 'similar', 'some words': 'and some others'}
        km = kinesis_batch_manager.KinesisBatchPutManager('', 'an id', 0)
        km.submit_record(test_record)
        self.assertEqual(len(km.records_to_send), 1, msg="Ensure only 1 record is added to the list")
        self.assertEqual(km.records_to_send[0],
                         {'Data': dumps(test_record), 'PartitionKey': 'hey there!'},
                         msg="Validation of the records content once it is submitted")


@patch('boto3_batch_utils.kinesis_batch_manager.boto3', Mock())
@patch('boto3_batch_utils.kinesis_batch_manager.boto3.client', Mock(return_value=Mock()))
class FlushRecordsToKinesis(unittest.TestCase):
    """
    Make sure that all messages are sent to kinesis and that the "records to send" list is then reset
    """

    @patch.object(kinesis_batch_manager.KinesisBatchPutManager, '_send_single_batch_to_kinesis', Mock())
    @patch.object(kinesis_batch_manager.KinesisBatchPutManager, '_split_records_to_send_into_batches', Mock())
    def test_one_batch(self):
        km = kinesis_batch_manager.KinesisBatchPutManager('', '', 0)
        km._split_records_to_send_into_batches.return_value = [[1]]
        km.flush_all_records_to_kinesis()
        km._split_records_to_send_into_batches.assert_called_once()
        km._send_single_batch_to_kinesis.assert_called_once_with([1])
        self.assertEqual(km.records_to_send, [])

    @patch.object(kinesis_batch_manager.KinesisBatchPutManager, '_send_single_batch_to_kinesis', Mock())
    @patch.object(kinesis_batch_manager.KinesisBatchPutManager, '_split_records_to_send_into_batches', Mock())
    def test_several_batches(self):
        km = kinesis_batch_manager.KinesisBatchPutManager('', '', 0)
        km._split_records_to_send_into_batches.return_value = [[1], [2], [3], [4], [5]]
        km.flush_all_records_to_kinesis()
        km._split_records_to_send_into_batches.assert_called_once()
        km._send_single_batch_to_kinesis.assert_has_calls([
            call([1]), call([2]), call([3]), call([4]), call([5]),
        ])
        self.assertEqual(km.records_to_send, [])
