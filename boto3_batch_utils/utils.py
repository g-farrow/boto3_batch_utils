from decimal import Decimal
from json import JSONEncoder, dumps, loads
import logging

logger = logging.getLogger()


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
        logger.debug("Parsing list item for decimals: {}".format(i))
        if isinstance(i, float):
            array[array.index(i)] = Decimal(str(i))
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
    # new_record = {}
    # for k, v in record.items():
    #     logger.debug("Parsing attribute '{}' for decimals: {}".format(k, v))
    #     if isinstance(v, float):
    #         new_record[k] = Decimal(str(v))
    #     elif isinstance(v, dict):
    #         new_record[k] = convert_floats_in_dict_to_decimals(v)
    #     elif isinstance(v, list):
    #         new_record[k] = convert_floats_in_list_to_decimals(v)
    #     else:
    #         new_record[k] = v
    # return new_record
    return loads(dumps(record, cls=DecimalEncoder))


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
