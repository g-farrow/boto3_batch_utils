from decimal import Decimal
from json import JSONEncoder


def chunks(array, chunk_size):
    """
    Yield successive chunks of a given list, as per chunk size
    :param array: Array - Array to be chunked up
    :param chunk_size: Int - Size of chunks required
    :return: Array - List of chunked arrays
    """
    for i in range(0, len(array), chunk_size):
        yield array[i:i + chunk_size]


def convert_floats_in_list_to_decimals(array):
    for i in array:
        if isinstance(i, float):
            array[array.index(i)] = Decimal(i)
        elif isinstance(i, dict):
            convert_floats_in_dict_to_decimals(i)
        elif isinstance(i, list):
            convert_floats_in_list_to_decimals(i)
    return array


def convert_floats_in_dict_to_decimals(record):
    """
    Floats are not valid object types for Dynamo, they must be converted to Decimals
    :param record:
    """
    new_record = {}
    for k, v in record.items():
        if isinstance(v, float):
            new_record[k] = Decimal(v)
        elif isinstance(v, dict):
            new_record[k] = convert_floats_in_dict_to_decimals(v)
        elif isinstance(v, list):
            new_record[k] = convert_floats_in_list_to_decimals(v)
        else:
            new_record[k] = v
    return new_record


class DecimalEncoder(JSONEncoder):
    """
    Helper class to convert a replace Decimal objects with floats during JSON conversion.
    """
    def default(self, o):
        if isinstance(o, Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super().default(o)
