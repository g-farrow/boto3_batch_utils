import logging

from boto3_batch_utils.Cloudwatch import CloudwatchBatchDispatcher, cloudwatch_dimension
from boto3_batch_utils.Dynamodb import DynamoBatchDispatcher
from boto3_batch_utils.Kinesis import KinesisBatchDispatcher
from boto3_batch_utils.SQS import SQSBatchDispatcher

__all__ = [
    'logger',
    'CloudwatchBatchDispatcher',
    'cloudwatch_dimension',
    'DynamoBatchDispatcher',
    'KinesisBatchDispatcher',
    'SQSBatchDispatcher'
]

__version__ = '1.5.2'

logger = logging.getLogger('boto3-batch-utils')
