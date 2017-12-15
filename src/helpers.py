import time
from datetime import datetime


def datetime_to_timestamp(datetime_value):
    """
    Converts a datetime string of the format 2017-12-15T14:01:10.123 to a unix timestamp.

    :param datetime_value: The datetime string to convert
    :return: Integer timestamp
    """
    return time.mktime(datetime.strptime(datetime_value, '%Y-%m-%dT%H:%M:%S.%f').timetuple())


def is_valid_tuple(data, length):
    """
    Returns true if the tuple has the expected length and does NOT contain None values.

    :param data: The tuple
    :param length: The expected length
    :return: True if valid, false otherwise
    """
    return len(data) == length and None not in data
