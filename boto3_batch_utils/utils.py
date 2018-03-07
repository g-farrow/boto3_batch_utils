

def chunks(array, chunk_size):
    """
    Yield successive chunks of a given list, as per chunk size
    :param array: Array - Array to be chunked up
    :param chunk_size: Int - Size of chunks required
    :return: Array - List of chunked arrays
    """
    for i in range(0, len(array), chunk_size):
        yield array[i:i + chunk_size]
