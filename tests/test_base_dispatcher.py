from unittest import TestCase
from unittest.mock import patch, Mock, call

from botocore.exceptions import ClientError

from boto3_batch_utils.Base import BaseDispatcher


class MockClient:

    def __init__(self, client_name):
        self.client_name = client_name + "_client"

    def send_lots(self):
        pass

    def send_one(self):
        pass

mock_boto3_interface_type_mapper = {
    'test_subject': 'client'
}


@patch('boto3_batch_utils.Base._boto3_interface_type_mapper', mock_boto3_interface_type_mapper)
@patch('boto3_batch_utils.Base.boto3.client', MockClient)
class Initialise(TestCase):

    def test_init(self):
        # mock_boto3 = Mock()
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=1,
                              flush_payload_on_max_batch_size=False)
        self.assertEqual('test_subject', base.subject_name)
        self.assertEqual('test_subject_client', base.subject.client_name)
        self.assertEqual('send_lots', base.batch_dispatch_method.__name__)
        self.assertEqual('send_one', base.individual_dispatch_method.__name__)
        self.assertEqual(1, base.max_batch_size)
        self.assertEqual(False, base.flush_payload_on_max_batch_size)
        self.assertEqual([], base.payload_list)


@patch('boto3_batch_utils.Base._boto3_interface_type_mapper', mock_boto3_interface_type_mapper)
@patch('boto3_batch_utils.Base.boto3.client', MockClient)
class SubmitPayload(TestCase):

    def test_when_payload_list_is_empty(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=1,
                              flush_payload_on_max_batch_size=False)
        base._flush_payload_selector = Mock()
        pl = "a"
        base.submit_payload(pl)
        self.assertEqual(["a"], base.payload_list)
        base._flush_payload_selector.assert_called_once()

    def test_when_payload_list_is_not_empty(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=1,
                              flush_payload_on_max_batch_size=False)
        base._flush_payload_selector = Mock()
        existing_payload_list = [1, 2, 3, 4, 5, {"6": "seven"}]
        base.payload_list = existing_payload_list
        pl = "a"
        base.submit_payload(pl)
        self.assertEqual([1, 2, 3, 4, 5, {"6": "seven"}, "a"], base.payload_list)
        base._flush_payload_selector.assert_called_once()


@patch('boto3_batch_utils.Base._boto3_interface_type_mapper', mock_boto3_interface_type_mapper)
@patch('boto3_batch_utils.Base.boto3.client', MockClient)
class PayloadSelectorWhenFlushOnMaxIsTrue(TestCase):

    def test_empty_payload_list(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=True)
        base.payload_list = []
        base._batch_send_payloads = Mock()
        base._flush_payload_selector()
        base._batch_send_payloads.assert_not_called()

    def test_payload_list_less_than_max_batch_size(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=True)
        base.payload_list = [1, 2]
        base._batch_send_payloads = Mock()
        base._flush_payload_selector()
        base._batch_send_payloads.assert_not_called()

    def test_payload_list_equal_max_batch_size(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=True)
        base.payload_list = [1, 2, 3]
        base._batch_send_payloads = Mock()
        base._flush_payload_selector()
        base._batch_send_payloads.assert_called_once_with([1, 2, 3])

    def test_payload_list_greater_than_max_batch_size(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=True)
        base.payload_list = [1, 2, 3, 4]
        base._batch_send_payloads = Mock()
        base._flush_payload_selector()
        base._batch_send_payloads.assert_not_called()


@patch('boto3_batch_utils.Base._boto3_interface_type_mapper', mock_boto3_interface_type_mapper)
@patch('boto3_batch_utils.Base.boto3.client', MockClient)
class PayloadSelectorWhenFlushOnMaxIsFalse(TestCase):

    def test_empty_payload_list(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        base.payload_list = []
        base._batch_send_payloads = Mock()
        base._flush_payload_selector()
        base._batch_send_payloads.assert_not_called()

    def test_payload_list_less_than_max_batch_size(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        base.payload_list = [1, 2]
        base._batch_send_payloads = Mock()
        base._flush_payload_selector()
        base._batch_send_payloads.assert_not_called()

    def test_payload_list_equal_max_batch_size(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        base.payload_list = [1, 2, 3]
        base._batch_send_payloads = Mock()
        base._flush_payload_selector()
        base._batch_send_payloads.assert_not_called()

    def test_payload_list_greater_than_max_batch_size(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        base.payload_list = [1, 2, 3, 4]
        base._batch_send_payloads = Mock()
        base._flush_payload_selector()
        base._batch_send_payloads.assert_not_called()


@patch('boto3_batch_utils.Base._boto3_interface_type_mapper', mock_boto3_interface_type_mapper)
@patch('boto3_batch_utils.Base.boto3.client', MockClient)
@patch('boto3_batch_utils.Base.chunks')
class FlushPayloads(TestCase):

    def test_empty_payload_list(self, mock_chunks):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        base.payload_list = []
        base._batch_send_payloads = Mock()
        mock_chunks.return_value = [[]]
        base.flush_payloads()
        base._batch_send_payloads.assert_not_called()
        self.assertEqual([], base.payload_list)

    def test_payload_partial_max_batch_size(self, mock_chunks):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        base.payload_list = [1, 2]
        base._batch_send_payloads = Mock()
        mock_chunks.return_value = [[1, 2]]
        base.flush_payloads()
        base._batch_send_payloads.assert_called_once_with([1, 2])
        self.assertEqual([], base.payload_list)

    def test_payload_equal_max_batch_size(self, mock_chunks):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        base.payload_list = [1, 2, 3]
        base._batch_send_payloads = Mock()
        mock_chunks.return_value = [[1, 2, 3]]
        base.flush_payloads()
        base._batch_send_payloads.assert_called_once_with([1, 2, 3])
        self.assertEqual([], base.payload_list)

    def test_payload_multiple_batches(self, mock_chunks):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        base.payload_list = [1, 2, 3, 4]
        base._batch_send_payloads = Mock()
        mock_chunks.return_value = [[1, 2, 3], [4]]
        base.flush_payloads()
        base._batch_send_payloads.assert_has_calls([call([1, 2, 3]), call([4])])
        self.assertEqual([], base.payload_list)


@patch('boto3_batch_utils.Base._boto3_interface_type_mapper', mock_boto3_interface_type_mapper)
@patch('boto3_batch_utils.Base.boto3.client', MockClient)
class BatchSendPayloads(TestCase):

    def test_empty_list(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        test_batch = []
        base.batch_dispatch_method = Mock(return_value="batch_response")
        base._process_batch_send_response = Mock()
        base._batch_send_payloads(test_batch)
        base.batch_dispatch_method.assert_called_once_with(test_batch)
        base._process_batch_send_response.assert_called_once_with("batch_response")

    def test_empty_dict(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        test_batch = {}
        base.batch_dispatch_method = Mock(return_value="batch_response")
        base._process_batch_send_response = Mock()
        base._batch_send_payloads(test_batch)
        base.batch_dispatch_method.assert_called_once_with(**test_batch)
        base._process_batch_send_response.assert_called_once_with("batch_response")

    def test_list(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        test_batch = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        base.batch_dispatch_method = Mock(return_value="batch_response")
        base._process_batch_send_response = Mock()
        base._batch_send_payloads(test_batch)
        base.batch_dispatch_method.assert_called_once_with(test_batch)
        base._process_batch_send_response.assert_called_once_with("batch_response")

    def test_dict(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        test_batch = {'something_to_process': [1, 2, 3, 4, 5, 6]}
        base.batch_dispatch_method = Mock(return_value="batch_response")
        base._process_batch_send_response = Mock()
        base._batch_send_payloads(test_batch)
        base.batch_dispatch_method.assert_called_once_with(**test_batch)
        base._process_batch_send_response.assert_called_once_with("batch_response")

    def test_list_client_error(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        test_batch = []
        base.batch_dispatch_method = Mock(side_effect=ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test"))
        base._process_batch_send_response = Mock()
        base._handle_client_error = Mock()
        with self.assertRaises(ClientError):
            base._batch_send_payloads(test_batch)
            base.batch_dispatch_method.assert_called_once_with(test_batch)
            base._handle_client_error.assert_called_once_with("An error occurred (Unknown) when calling the A Test operation: Unknown", test_batch)
            base._process_batch_send_response.assert_not_called()

    def test_dict_client_error(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        test_batch = {}
        base.batch_dispatch_method = Mock(side_effect=ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test"))
        base._process_batch_send_response = Mock()
        base._handle_client_error = Mock()
        with self.assertRaises(ClientError):
            base._batch_send_payloads(test_batch)
            base.batch_dispatch_method.assert_called_once_with(**test_batch)
            base._handle_client_error.assert_called_once_with("An error occurred (Unknown) when calling the A Test operation: Unknown", test_batch)
            base._process_batch_send_response.assert_not_called()


@patch('boto3_batch_utils.Base._boto3_interface_type_mapper', mock_boto3_interface_type_mapper)
@patch('boto3_batch_utils.Base.boto3.client', MockClient)
class SendIndividualPayload(TestCase):

    def test_successful_send_non_dict(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        base.individual_dispatch_method = Mock()
        test_payload = "abc"
        base._send_individual_payload(test_payload)
        base.individual_dispatch_method.assert_called_once_with(test_payload)

    def test_successfully_sent_after_4_failures(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        client_error = ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test")
        base.individual_dispatch_method = Mock(side_effect=[client_error, client_error, client_error, client_error, ""])
        test_payload = "abc"
        base._send_individual_payload(test_payload)
        base.individual_dispatch_method.assert_has_calls([
            call(test_payload),
            call(test_payload),
            call(test_payload),
            call(test_payload),
            call(test_payload)
        ])

    def test_non_dict_raises_client_error_sent_after_5_failures(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        client_error = ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test")
        base.individual_dispatch_method = Mock(side_effect=[client_error, client_error, client_error, client_error,
                                                            client_error])
        test_payload = "abc"
        with self.assertRaises(ClientError):
            base._send_individual_payload(test_payload)
        base.individual_dispatch_method.assert_has_calls([
            call(test_payload),
            call(test_payload),
            call(test_payload),
            call(test_payload),
            call(test_payload)
        ])


    def test_successful_send_dict(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        base.individual_dispatch_method = Mock()
        test_payload = {"abc": 123}
        base._send_individual_payload(test_payload)
        base.individual_dispatch_method.assert_called_once_with(**test_payload)

    def test_successfully_sent_after_4_failures_dict(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        client_error = ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test")
        base.individual_dispatch_method = Mock(side_effect=[client_error, client_error, client_error, client_error, ""])
        test_payload = {"abc": 123}
        base._send_individual_payload(test_payload)
        base.individual_dispatch_method.assert_has_calls([
            call(**test_payload),
            call(**test_payload),
            call(**test_payload),
            call(**test_payload),
            call(**test_payload)
        ])

    def test_dict_raises_client_error_after_5_failures(self):
        base = BaseDispatcher('test_subject', 'send_lots', 'send_one', batch_size=3,
                              flush_payload_on_max_batch_size=False)
        client_error = ClientError({"Error": {"message": "Something went wrong", "code": 0}}, "A Test")
        base.individual_dispatch_method = Mock(side_effect=[client_error, client_error, client_error, client_error,
                                                            client_error])
        test_payload = {"abc": 123}
        with self.assertRaises(ClientError):
            base._send_individual_payload(test_payload)
        base.individual_dispatch_method.assert_has_calls([
            call(**test_payload),
            call(**test_payload),
            call(**test_payload),
            call(**test_payload),
            call(**test_payload)
        ])
