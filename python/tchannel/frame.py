from __future__ import absolute_import

from .context import Context
from .exceptions import ProtocolException
from .mapping import get_message_class
from .parser import read_number
from .parser import write_number


class Frame(object):
    """Perform operations on a single TChannel frame."""
    SIZE_WIDTH = 2
    ID_WIDTH = 4
    TYPE_WIDTH = 1
    FLAGS_WIDTH = 1
    PRELUDE_SIZE = 0x10  # this many bytes of framing before payload

    # 8 bytes are reserved
    RESERVED_WIDTH = 8
    RESERVED_PADDING = b'\x00' * RESERVED_WIDTH

    BEFORE_ID_WIDTH = 1
    BEFORE_ID_PADDING = b'\x00' * BEFORE_ID_WIDTH

    def __init__(self, message, message_id):
        self._message = message
        self._message_id = message_id

    @classmethod
    def decode(cls, stream, message_length=None):
        """Decode a sequence of bytes into a frame and message.

        :param stream: a byte stream
        :param message_length: length of the message in bytes including framing
        """
        if message_length is None:
            message_length = read_number(stream, cls.SIZE_WIDTH)

        if message_length < cls.PRELUDE_SIZE:
            raise ProtocolException(
                'Illegal frame length: %d' % message_length
            )

        message_type = read_number(stream, cls.TYPE_WIDTH)
        message_class = get_message_class(message_type)
        if not message_class:
            raise ProtocolException('Unknown message type: %d' % message_type)

        # Throw away this many bytes
        stream.read(cls.BEFORE_ID_WIDTH)

        message_id = read_number(stream, cls.ID_WIDTH)

        stream.read(cls.RESERVED_WIDTH)

        message = message_class()
        remaining_bytes = message_length - cls.PRELUDE_SIZE
        if remaining_bytes:
            message.parse(stream, remaining_bytes)
        context = Context(message_id=message_id, message=message)
        return context

    def write(self, connection, callback=None):
        """Write a frame out to a connection."""
        payload = bytearray()
        self._message.serialize(payload)
        payload_length = len(payload)

        header_bytes = bytearray()

        header_bytes.extend(write_number(
            payload_length + self.PRELUDE_SIZE,
            self.SIZE_WIDTH
        ))

        header_bytes.extend(write_number(
            self._message.message_type,
            self.TYPE_WIDTH
        ))

        header_bytes.extend(self.BEFORE_ID_PADDING)

        header_bytes.extend(write_number(
            self._message_id,
            self.ID_WIDTH
        ))

        # 8 bytes of reserved data
        header_bytes.extend(self.RESERVED_PADDING)

        # Then the payload
        header_bytes.extend(payload)
        return connection.write(header_bytes, callback=callback)
