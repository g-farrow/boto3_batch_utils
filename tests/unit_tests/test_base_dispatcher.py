from unittest import TestCase
from unittest.mock import patch, Mock, call

from botocore.exceptions import ClientError

from boto3_batch_utils.Base import BaseDispatcher


class MockClient:

    def __init__(self, client_name, **kwargs):
        self.client_name = client_name + "_client"
        self.kwargs = kwargs or {}

    def send_lots(self, batch):
        pass

    def send_one(self, payload):
        pass


mock_boto3_interface_type_mapper = {
    'test_subject': 'client'
}


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
class InitialiseBatchUtilsClient(TestCase):

    def test_init(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=1)
        self.assertEqual('test_subject', base.aws_service_name)
        self.assertIsNone(base._aws_service)
        self.assertEqual('send_lots', base.batch_dispatch_method)
        self.assertIsNone(base._batch_dispatch_method)
        self.assertEqual('send_one', base.individual_dispatch_method)
        self.assertIsNone(base._individual_dispatch_method)
        self.assertEqual(1, base.max_batch_size)
        self.assertIsNone(base._aws_service_batch_max_payloads)
        self.assertIsNone(base._aws_service_message_max_bytes)
        self.assertIsNone(base._aws_service_batch_max_bytes)
        self.assertEqual({}, base._batch_payload_wrapper)
        self.assertIsNone(base._batch_payload)
        self.assertEqual(0, base._batch_payload_wrapper_byte_size)
        self.assertEqual([], base.unprocessed_items)


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
class TestValidateInitialisation(TestCase):

    def test_initialisation_valid(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=1)
        base._aws_service_batch_max_payloads = 1

        base._validate_initialisation()

    def test_max_batch_too_large_raises_exception(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=2)
        base._aws_service_batch_max_payloads = 1

        with self.assertRaises(ValueError) as context:
            base._validate_initialisation()
        self.assertIn("Requested max_batch_size '2' exceeds the test_subject maximum", str(context.exception))


@patch('boto3_batch_utils.Base._boto3_interface_type_mapper', mock_boto3_interface_type_mapper)
@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
class InitialiseAwsClient(TestCase):

    def test_init(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=1)
        base._initialise_aws_client()
        self.assertEqual('test_subject_client', base._aws_service.client_name)
        self.assertEqual('send_lots', base._batch_dispatch_method.__name__)
        self.assertEqual('send_one', base._individual_dispatch_method.__name__)
    
    def test_boto3_overrides(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=1, endpoint_url='https://dummy_endpoint:54321/', aws_session_token='session_token')
        base._initialise_aws_client()
        self.assertEqual('https://dummy_endpoint:54321/', base._aws_service.kwargs['endpoint_url'])
        self.assertEqual('session_token', base._aws_service.kwargs['aws_session_token'])



@patch('boto3_batch_utils.Base._boto3_interface_type_mapper', mock_boto3_interface_type_mapper)
@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
class SubmitPayload(TestCase):

    def test_when_payload_list_is_empty(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=1)
        base._aws_service_message_max_bytes = 15
        base._aws_service_batch_max_bytes = 15
        base._append_payload_to_current_batch = Mock()
        base._flush_payload_selector = Mock()
        pl = {"a": True}
        base.submit_payload(pl)
        base._append_payload_to_current_batch.assert_called_once_with(pl)
        base._flush_payload_selector.assert_called_once()

    def test_when_payload_is_over_byte_size(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=1)
        base._aws_service_message_max_bytes = 10
        base._aws_service_batch_max_bytes = 15
        base._append_payload_to_current_batch = Mock()
        base._flush_payload_selector = Mock()
        pl = {"a": True}
        with self.assertRaises(ValueError) as context:
            base.submit_payload(pl)
        self.assertIn("exceeds the maximum payload size", str(context.exception))
        base._append_payload_to_current_batch.assert_not_called()
        base._flush_payload_selector.assert_not_called()

    def test_when_payload_is_equal_to_byte_size(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=1)
        base._aws_service_message_max_bytes = 11
        base._aws_service_batch_max_bytes = 15
        base._validate_payload_byte_size = Mock(return_value=True)
        base._append_payload_to_current_batch = Mock()
        base._flush_payload_selector = Mock()
        pl = {"a": True}
        base.submit_payload(pl)
        base._append_payload_to_current_batch.assert_called_once_with(pl)
        base._flush_payload_selector.assert_called_once()


@patch('boto3_batch_utils.Base._boto3_interface_type_mapper', mock_boto3_interface_type_mapper)
@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
class PayloadSelectorWhenFlushOnMaxIsTrue(TestCase):

    def test_empty_payload_list(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        base._batch_payload = []
        base._batch_send_payloads = Mock()
        base._flush_payload_selector()
        base._batch_send_payloads.assert_not_called()

    def test_payload_list_less_than_max_batch_size(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        base._batch_payload = [1, 2]
        base._batch_send_payloads = Mock()
        base._flush_payload_selector()
        base._batch_send_payloads.assert_not_called()

    def test_payload_list_equal_max_batch_size(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        base._batch_payload = [1, 2, 3]
        base._batch_send_payloads = Mock()
        base._flush_payload_selector()
        base._batch_send_payloads.assert_called_once_with([1, 2, 3])

    def test_payload_list_greater_than_max_batch_size(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        base._batch_payload = [1, 2, 3, 4]
        base._batch_send_payloads = Mock()
        base._flush_payload_selector()
        base._batch_send_payloads.assert_has_calls([call([1, 2, 3]), call([4])])


@patch('boto3_batch_utils.Base._boto3_interface_type_mapper', mock_boto3_interface_type_mapper)
@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
@patch('boto3_batch_utils.Base.chunks')
class FlushPayloads(TestCase):

    def test_empty_payload_list(self, mock_chunks):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        base._batch_payload = []
        base._initialise_aws_client = Mock()
        base._batch_send_payloads = Mock()
        mock_chunks.return_value = [[]]
        base.flush_payloads()
        base._initialise_aws_client.assert_called_once()
        base._batch_send_payloads.assert_not_called()
        self.assertEqual([], base._batch_payload)

    def test_payload_partial_max_batch_size(self, mock_chunks):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        base._batch_payload = [1, 2]
        base._initialise_aws_client = Mock()
        base._batch_send_payloads = Mock()
        mock_chunks.return_value = [[1, 2]]
        base.flush_payloads()
        base._initialise_aws_client.assert_called_once()
        base._batch_send_payloads.assert_called_once_with([1, 2])
        self.assertEqual([], base._batch_payload)

    def test_payload_equal_max_batch_size(self, mock_chunks):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        base._batch_payload = [1, 2, 3]
        base._initialise_aws_client = Mock()
        base._batch_send_payloads = Mock()
        mock_chunks.return_value = [[1, 2, 3]]
        base.flush_payloads()
        base._initialise_aws_client.assert_called_once()
        base._batch_send_payloads.assert_called_once_with([1, 2, 3])
        self.assertEqual([], base._batch_payload)

    def test_payload_multiple_batches(self, mock_chunks):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        base._batch_payload = [1, 2, 3, 4]
        base._initialise_aws_client = Mock()
        base._batch_send_payloads = Mock()
        mock_chunks.return_value = [[1, 2, 3], [4]]
        base.flush_payloads()
        base._initialise_aws_client.assert_called_once()
        base._batch_send_payloads.assert_has_calls([call([1, 2, 3]), call([4])])
        self.assertEqual([], base._batch_payload)

    def test_unprocessed_items_are_returned(self, mock_chunks):
        test_unprocessed_items = ['abc1', 'cde2', 'efg3']
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        base.unprocessed_items = test_unprocessed_items
        base._batch_payload = [1, 2, 3, 4]
        base._initialise_aws_client = Mock()
        base._batch_send_payloads = Mock()
        mock_chunks.return_value = [[1, 2, 3], [4]]

        response = base.flush_payloads()

        base._initialise_aws_client.assert_called_once()
        base._batch_send_payloads.assert_has_calls([call([1, 2, 3]), call([4])])
        self.assertEqual([], base._batch_payload)
        self.assertEqual(test_unprocessed_items, response)


@patch('boto3_batch_utils.Base._boto3_interface_type_mapper', mock_boto3_interface_type_mapper)
@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
class BatchSendPayloads(TestCase):

    def test_empty_list(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        test_batch = []
        base._batch_dispatch_method = Mock(return_value="batch_response")
        base._process_batch_send_response = Mock()
        base._batch_send_payloads(test_batch)
        base._batch_dispatch_method.assert_called_once_with(test_batch)
        base._process_batch_send_response.assert_called_once_with("batch_response")

    def test_empty_dict(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        test_batch = {}
        base._batch_dispatch_method = Mock(return_value="batch_response")
        base._process_batch_send_response = Mock()
        base._batch_send_payloads(test_batch)
        base._batch_dispatch_method.assert_called_once_with(**test_batch)
        base._process_batch_send_response.assert_called_once_with("batch_response")

    def test_list(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        test_batch = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        base._batch_dispatch_method = Mock(return_value="batch_response")
        base._process_batch_send_response = Mock()
        base._batch_send_payloads(test_batch)
        base._batch_dispatch_method.assert_called_once_with(test_batch)
        base._process_batch_send_response.assert_called_once_with("batch_response")

    def test_dict(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        test_batch = {'something_to_process': [1, 2, 3, 4, 5, 6]}
        base._batch_dispatch_method = Mock(return_value="batch_response")
        base._process_batch_send_response = Mock()
        base._batch_send_payloads(test_batch)
        base._batch_dispatch_method.assert_called_once_with(**test_batch)
        base._process_batch_send_response.assert_called_once_with("batch_response")

    def test_list_batch_send_failures_sent_to_unprocessed_items(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        test_batch = ["abc", "cde"]
        base._batch_dispatch_method = Mock(side_effect=ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test"))
        base._process_batch_send_response = Mock()
        base._handle_client_error = Mock()
        base._unpack_failed_batch_to_unprocessed_items = Mock()

        base._batch_send_payloads(test_batch)

        base._batch_dispatch_method.assert_has_calls([call(test_batch), call(test_batch), call(test_batch),
                                                      call(test_batch), call(test_batch)])
        base._process_batch_send_response.assert_not_called()
        base._unpack_failed_batch_to_unprocessed_items.assert_called_once_with(test_batch)

    def test_dict_batch_send_failures_sent_to_unporcessed_items(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        test_batch = {'something': 'batchy'}
        base._batch_dispatch_method = Mock(
            side_effect=[
                ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test"),
                ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test"),
                ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test"),
                ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test"),
                ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test"),
                ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test")
            ]
        )
        base._process_batch_send_response = Mock()
        base._handle_client_error = Mock()
        base._unpack_failed_batch_to_unprocessed_items = Mock()

        base._batch_send_payloads(test_batch)

        base._batch_dispatch_method.assert_has_calls([call(**test_batch), call(**test_batch), call(**test_batch),
                                                      call(**test_batch), call(**test_batch)])
        base._process_batch_send_response.assert_not_called()
        base._unpack_failed_batch_to_unprocessed_items.assert_called_once_with(test_batch)


@patch('boto3_batch_utils.Base._boto3_interface_type_mapper', mock_boto3_interface_type_mapper)
@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
class SendIndividualPayload(TestCase):

    def test_successful_send_non_dict(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        base._individual_dispatch_method = Mock()
        test_payload = "abc"
        base._send_individual_payload(test_payload)
        base._individual_dispatch_method.assert_called_once_with(test_payload)

    def test_successfully_sent_after_4_failures(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        client_error = ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test")
        base._individual_dispatch_method = Mock(side_effect=[client_error, client_error, client_error, client_error, ""])
        test_payload = "abc"
        base._send_individual_payload(test_payload)
        base._individual_dispatch_method.assert_has_calls([
            call(test_payload),
            call(test_payload),
            call(test_payload),
            call(test_payload),
            call(test_payload)
        ])

    def test_non_dict_added_to_unprocessed_items_after_5_failures(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        client_error = ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test")
        base._individual_dispatch_method = Mock(side_effect=[client_error, client_error, client_error, client_error,
                                                             client_error])

        test_payload = "abc"
        base._send_individual_payload(test_payload)
        base._individual_dispatch_method.assert_has_calls([
            call(test_payload),
            call(test_payload),
            call(test_payload),
            call(test_payload),
            call(test_payload)
        ])
        self.assertEqual([test_payload], base.unprocessed_items)


    def test_successful_send_dict(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        base._individual_dispatch_method = Mock()
        test_payload = {"abc": 123}
        base._send_individual_payload(test_payload)
        base._individual_dispatch_method.assert_called_once_with(**test_payload)

    def test_successfully_sent_after_4_failures_dict(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        client_error = ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test")
        base._individual_dispatch_method = Mock(side_effect=[client_error, client_error, client_error, client_error, ""])
        test_payload = {"abc": 123}
        base._send_individual_payload(test_payload)
        base._individual_dispatch_method.assert_has_calls([
            call(**test_payload),
            call(**test_payload),
            call(**test_payload),
            call(**test_payload),
            call(**test_payload)
        ])

    def test_dict_added_to_unprocessed_items_after_5_failures(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=3)
        client_error = ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test")
        base._individual_dispatch_method = Mock(side_effect=[client_error, client_error, client_error, client_error,
                                                             client_error])
        test_payload = {"abc": 123}
        base._send_individual_payload(test_payload)
        base._individual_dispatch_method.assert_has_calls([
            call(**test_payload),
            call(**test_payload),
            call(**test_payload),
            call(**test_payload),
            call(**test_payload)
        ])
        self.assertEqual([test_payload], base.unprocessed_items)


@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.boto3', Mock())
@patch('boto3_batch_utils.Base.get_byte_size_of_dict_or_list')
class TestValidatePayloadByteSize(TestCase):

    def test_less_than_max(self, mock_get_byte_size_of_dict_or_list):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=1)
        base._aws_service_message_max_bytes = 2
        mock_get_byte_size_of_dict_or_list.return_value = 1
        test_pl = {'stuff': True}

        base._validate_payload_byte_size(test_pl)

        mock_get_byte_size_of_dict_or_list.assert_has_calls([call({}), call(test_pl)], any_order=True)

    def test_equals_max(self, mock_get_byte_size_of_dict_or_list):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=1)
        base._aws_service_message_max_bytes = 1
        mock_get_byte_size_of_dict_or_list.return_value = 1
        test_pl = {'stuff': True}

        base._validate_payload_byte_size(test_pl)

        mock_get_byte_size_of_dict_or_list.assert_has_calls([call({}), call(test_pl)], any_order=True)

    def test_more_than_max(self, mock_get_byte_size_of_dict_or_list):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', max_batch_size=1)
        base._aws_service_message_max_bytes = 1
        mock_get_byte_size_of_dict_or_list.return_value = 2
        test_pl = {'stuff': True}

        with self.assertRaises(ValueError) as context:
            base._validate_payload_byte_size(test_pl)
        self.assertIn("exceeds the maximum payload size", str(context.exception))

        mock_get_byte_size_of_dict_or_list.assert_has_calls([call({}), call(test_pl)], any_order=True)
