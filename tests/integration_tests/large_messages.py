from boto3_batch_utils.utils import get_byte_size_of_dict_or_list


def create_dict_of_specific_byte_size(initial_dict: dict, desired_byte_size: int):
    new_dict = initial_dict
    new_dict['extra_content_to_achieve_required_byte_size'] = ""
    initial_byte_size = get_byte_size_of_dict_or_list(initial_dict)
    additional_required_bytes = desired_byte_size - initial_byte_size
    for _ in range(0, additional_required_bytes):
        new_dict['extra_content_to_achieve_required_byte_size'] += "x"
    return new_dict
