from __future__ import absolute_import

import struct


def get_number_format(size):
    if size == 1:
        return '>B'
    elif size == 2:
        return '>H'
    elif size == 4:
        return '>I'
    elif size == 8:
        return '>Q'
    else:
        raise ValueError('size must be 1, 2, 4, or 8, not %d' % size)


def write_number(value, size):
    """Write a big-endian short."""
    return struct.pack(get_number_format(size), value)


def read_number_string(string, size):
    return struct.unpack(get_number_format(size), string)[0]


def read_number(buff, size):
    """Read a big-endian number off the byte stream."""
    return struct.unpack(get_number_format(size), buff.read(size))[0]


def read_short(buff):
    """Read two bytes in big-endian and return an unsigned integer."""
    return read_number(buff, 2)


def read_variable_length_key(buff, key_size, decode=True):
    """Read a variable-length key from a stream.

    Returns tuple of (value, bytes read).
    """
    key_bytes = read_number(buff, key_size)
    if key_bytes:
        value = buff.read(key_bytes)
        return_value = value.decode('utf-8') if decode else value
    else:
        return_value = None
    return return_value, (key_bytes + key_size)


def read_key_value(buff, key_size, value_size=None):
    """Read a variable-length key-value pair from a stream.

    Returns tuple of (key, value, bytes read).
    """
    if value_size is None:
        value_size = key_size

    key, key_bytes = read_variable_length_key(buff, key_size)
    if value_size > 0:
        value, value_bytes = read_variable_length_key(buff, value_size)
    else:
        value, value_bytes = None, 0

    return key, value, (key_bytes + value_bytes)


def write_variable_length_key(stream, value, value_size, encode=True):
    """Write a length followed by that many bytes."""
    if value:
        encoded_value = value.encode('utf-8') if encode else value
        value_length = len(encoded_value)
    else:
        encoded_value = None
        value_length = 0

    stream.extend(write_number(value_length, value_size))
    if encoded_value:
        stream.extend(encoded_value)


def write_key_value(key, value, key_size, value_size=None):
    """Write a variable-length key-value pair.

    Returns an array of bytes.
    """
    if value_size is None:
        value_size = key_size

    stream = bytearray()
    write_variable_length_key(
        stream,
        key,
        key_size
    )

    if value:
        write_variable_length_key(stream, value, value_size)
    else:
        stream.extend(write_number(0, value_size))
    return stream
