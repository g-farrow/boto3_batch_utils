from unittest import TestCase
from unittest.mock import patch
from decimal import Decimal
import json
from boto3_batch_utils import utils


class TestChunks(TestCase):

    def test_array_smaller_than_chunk_size(self):
        array = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        batch_size = 20
        self.assertEqual([array], list(utils.chunks(array, batch_size)))

    def test_array_equal_to_chunk_size(self):
        array = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        batch_size = 10
        self.assertEqual([array], list(utils.chunks(array, batch_size)))

    def test_array_greater_than_chunk_size_but_less_than_double(self):
        array = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        batch_size = 8
        self.assertEqual([[1, 2, 3, 4, 5, 6, 7, 8], [9, 10]], list(utils.chunks(array, batch_size)))

    def test_array_greater_than_double_chunk_size_smaller_than_triple_chunk_size(self):
        array = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        batch_size = 4
        self.assertEqual([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10]], list(utils.chunks(array, batch_size)))


class TestConvertFloatsInListToDecimal(TestCase):

    def test_the_array_is_empty(self):
        array = []
        new_array = utils.convert_floats_in_list_to_decimals(array)
        self.assertEqual([], new_array)

    def test_single_item_in_array_is_not_a_float(self):
        array = [True]
        new_array = utils.convert_floats_in_list_to_decimals(array)
        self.assertEqual(array, new_array)

    def test_single_item_in_array_is_a_float(self):
        array = [float(5.0)]
        new_array = utils.convert_floats_in_list_to_decimals(array)
        self.assertEqual([Decimal(5.0)], new_array)

    def test_multiple_items_in_array_none_are_a_float(self):
        array = ["a", "b", "c", Decimal(6.7)]
        new_array = utils.convert_floats_in_list_to_decimals(array)
        self.assertEqual(array, new_array)

    def test_multiple_items_in_array_some_are_floats(self):
        array = ["a", "b", float(2.2), "c", float(5.5), "d"]
        new_array = utils.convert_floats_in_list_to_decimals(array)
        self.assertEqual(["a", "b", Decimal(str(2.2)), "c", Decimal(str(5.5)), "d"], new_array)

    def test_multiple_items_in_array_all_are_floats(self):
        array = [float(5), float(50), float(0.01), float(2.2), float(1.0), float(990)]
        new_array = utils.convert_floats_in_list_to_decimals(array)
        self.assertEqual([
            Decimal(str(5)), Decimal(str(50)), Decimal(str(0.01)),
            Decimal(str(2.2)), Decimal(str(1.0)), Decimal(str(990))], new_array)

    def test_some_items_are_lists_containing_floats(self):
        array = ["a", "b", ["rr", float(2.2)], "c", ["dd", ["gh", float(5.5)]], "d"]
        new_array = utils.convert_floats_in_list_to_decimals(array)
        self.assertEqual(["a", "b", ["rr", Decimal(str(2.2))], "c", ["dd", ["gh", Decimal(str(5.5))]], "d"], new_array)

    @patch('boto3_batch_utils.utils.convert_floats_in_dict_to_decimals')
    def test_some_items_are_dictionaries(self, mock_convert_floats_to_decimals_in_dict):
        mock_convert_floats_to_decimals_in_dict.side_effect = [{"sss": True}]
        array = ["a", "b", "c", {"sss": True}]
        new_array = utils.convert_floats_in_list_to_decimals(array)
        self.assertEqual(array, new_array)
        mock_convert_floats_to_decimals_in_dict.assert_called_once_with({"sss": True}, level=1)


class TestConvertFloatsInDictToDecimal(TestCase):

    def test_empty_dict(self):
        d = {}
        new_d = utils.convert_floats_in_dict_to_decimals(d)
        self.assertEqual(d, new_d)

    def test_dict_with_no_floats(self):
        d = {'adsefsvs': True, 'dfgsdzfvzdsv': Decimal(4.4)}
        new_d = utils.convert_floats_in_dict_to_decimals(d)
        self.assertEqual(d, new_d)

    def test_dict_with_floats(self):
        d = {'sgervv': float(6.7), 'fsrgs': False, 'csfwcda': None}
        new_d = utils.convert_floats_in_dict_to_decimals(d)
        self.assertEqual({'sgervv': Decimal(str(6.7)),
                          'fsrgs': False, 'csfwcda': None}, new_d)

    def test_dict_with_nested_dicts_wiht_floats(self):
        d = {'adsefsvs': True, 'dfgsdzfvzdsv': {'sgervv': float(6.7), 'fsrgs': False, 'csfwcda': None}}
        new_d = utils.convert_floats_in_dict_to_decimals(d)
        self.assertEqual({'adsefsvs': True, 'dfgsdzfvzdsv': {
            'sgervv': Decimal(str(6.7)), 'fsrgs': False, 'csfwcda': None}},
                         new_d)

    @patch('boto3_batch_utils.utils.convert_floats_in_list_to_decimals')
    def test_dict_with_nested_lists(self, mock_convert_floats_in_list_to_decimals):
        mock_convert_floats_in_list_to_decimals.side_effect = [[Decimal(3.4), Decimal(66.9)]]
        d = {'ersrgsed': 'sgsdvfzdf', 'crvzvf': [Decimal(3.4), float(66.9)]}
        new_d = utils.convert_floats_in_dict_to_decimals(d)
        self.assertEqual({'ersrgsed': 'sgsdvfzdf', 'crvzvf': [
            Decimal(3.399999999999999911182158029987476766109466552734375),
            Decimal(66.900000000000005684341886080801486968994140625)
        ]}, new_d)
        mock_convert_floats_in_list_to_decimals.assert_called_once_with([Decimal(3.4), float(66.9)], level=1)


class TestGetByteSizeOfString(TestCase):

    def test_one(self):
        test_string = "1234567890"
        response = utils.get_byte_size_of_string(test_string)
        self.assertEqual(10, response)

    def test_two(self):
        test_string = "n*ZSLt%HsC$tG!gd!*xL3SrF!30&PiVN3*&e%bN#2qZ317f2^nUUNpphmDBSwOl*qk*tPV#l6$k0Mzxg$*dK2G7s$J!9aNQc&vK"
        response = utils.get_byte_size_of_string(test_string)
        self.assertEqual(99, response)

    def test_three(self):
        test_string = json.dumps({'dict': True, 'complex': 0, 'stuff': 'etc'})
        response = utils.get_byte_size_of_string(test_string)
        self.assertEqual(44, response)


@patch("boto3_batch_utils.utils.get_byte_size_of_string", return_value=5)
class TestGetByteSizeOfDict(TestCase):

    def test_dict(self, mock_get_byte_size_of_string):
        test_dict = {'dict': True, 'complex': 0, 'stuff': 'etc'}

        response = utils.get_byte_size_of_dict_or_list(test_dict)

        mock_get_byte_size_of_string.assert_called_once_with('{"dict": true, "complex": 0, "stuff": "etc"}')
        self.assertEqual(5, response)

    def test_list(self, mock_get_byte_size_of_string):
        test_dict = [{'dict': True, 'complex': 0, 'stuff': 'etc'}]

        response = utils.get_byte_size_of_dict_or_list(test_dict)

        mock_get_byte_size_of_string.assert_called_once_with('[{"dict": true, "complex": 0, "stuff": "etc"}]')
        self.assertEqual(5, response)
