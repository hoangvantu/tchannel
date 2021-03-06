from __future__ import absolute_import
import pytest

from tchannel import exceptions
from tchannel.frame import Frame
from tchannel.io import BytesIO
from tchannel.parser import read_number
from tchannel.messages.types import Types


class _FakeMessage(object):
    message_type = 0x30

    def serialize(self, out):
        """Serialize 0-bytes to ``out``."""
        return


@pytest.fixture
def dummy_frame():
    return bytearray([
        0, 16,  # Size
        0,  # type
        0,  # reserved
        0, 0, 0, 1,  # ID
        0, 0, 0, 0, 0, 0, 0, 0  # reserved padding
    ])


def test_empty_message(connection):
    """Verify size is set properly for an empty message."""
    message_id = 42
    frame = Frame(
        message=_FakeMessage(),
        message_id=message_id,
    )

    frame.write(connection)

    value = BytesIO(connection.getvalue())

    assert read_number(value, 2) == frame.PRELUDE_SIZE
    assert read_number(value, 1) == _FakeMessage.message_type
    value.read(1)  # throw away reserved bit
    assert read_number(value, 4) == message_id


def test_decode_empty_buffer():
    """Verify we raise on invalid buffers."""
    with pytest.raises(exceptions.ProtocolException):
        Frame.decode(BytesIO(b'\x00\x00\x00\x00'))


def test_decode_with_message_length(dummy_frame):
    """Verify we can pre-flight a message size."""
    dummy_frame[2] = Types.PING_REQ
    Frame.decode(BytesIO(dummy_frame[2:]), len(dummy_frame))


def test_decode_invalid_message_id(dummy_frame):
    """Verify we raise on invalid message IDs."""
    dummy_frame[8] = 55  # not a real message type
    with pytest.raises(exceptions.ProtocolException):
        Frame.decode(BytesIO(dummy_frame))


def test_decode_ping(dummy_frame):
    """Verify we can decode a ping message."""
    dummy_frame[2] = Types.PING_REQ
    Frame.decode(BytesIO(dummy_frame))
