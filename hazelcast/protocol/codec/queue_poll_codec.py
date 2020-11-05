from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x030500
_REQUEST_MESSAGE_TYPE = 197888
# hex: 0x030501
_RESPONSE_MESSAGE_TYPE = 197889

_REQUEST_TIMEOUT_MILLIS_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_TIMEOUT_MILLIS_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(name, timeout_millis):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_long(buf, _REQUEST_TIMEOUT_MILLIS_OFFSET, timeout_millis)
    StringCodec.encode(message, name)
    return message


def decode_response(message):
    message.next_frame()
    return CodecUtil.decode_nullable(message, DataCodec.decode)
