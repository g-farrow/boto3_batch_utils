from decimal import Decimal
from json import JSONEncoder
import logging
import json
from datetime import date, datetime


logger = logging.getLogger('boto3-batch-utils')


def chunks(array, chunk_size):
    """
    Yield successive chunks of a given list, as per chunk size
    :param array: Array - Array to be chunked up
    :param chunk_size: Int - Size of chunks required
    :return: Array - List of chunked arrays
    """
    for i in range(0, len(array), chunk_size):
        yield array[i:i + chunk_size]


def convert_floats_in_list_to_decimals(array, level=0):
    for i in array:
        logger.debug(f"Parsing list item for decimals (level: {level}): {i}")
        if isinstance(i, float):
            array[array.index(i)] = Decimal(str(i))
        elif isinstance(i, dict):
            array[array.index(i)] = convert_floats_in_dict_to_decimals(i, level=level+1)
        elif isinstance(i, list):
            array[array.index(i)] = convert_floats_in_list_to_decimals(i, level=level+1)
    return array


def convert_floats_in_dict_to_decimals(record, level=0):
    """
    Floats are not valid object types for Dynamo, they must be converted to Decimals
    :param record:
    """
    new_record = {}
    logger.debug(f"Processing dict (level: {level}): {record}")
    for k, v in record.items():
        logger.debug(f"Parsing attribute '{k}' for decimals: {v} ({type(v)})")
        if isinstance(v, float):
            new_record[k] = Decimal(str(v))
        elif isinstance(v, dict):
            new_record[k] = convert_floats_in_dict_to_decimals(v, level=level+1)
            logger.debug(f"New dict returned: {new_record[k]}")
        elif isinstance(v, list):
            new_record[k] = convert_floats_in_list_to_decimals(v, level=level+1)
        else:
            new_record[k] = v
        logger.debug(f"New dict: {new_record}")
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


def default(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat()


def get_byte_size_of_string(string: str) -> int:
    """
    Return the number of bytes of a string
    """
    return len(string.encode('utf-8'))


def get_byte_size_of_dict_or_list(d: dict) -> int:
    """
    Return the number of bytes of a string
    """
    return get_byte_size_of_string(json.dumps(d, default=default))
