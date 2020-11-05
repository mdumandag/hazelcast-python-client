from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x010400
_REQUEST_MESSAGE_TYPE = 66560
# hex: 0x010401
_RESPONSE_MESSAGE_TYPE = 66561

_REQUEST_THREAD_ID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_THREAD_ID_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(name, key, value, thread_id):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_long(buf, _REQUEST_THREAD_ID_OFFSET, thread_id)
    StringCodec.encode(message, name)
    DataCodec.encode(message, key)
    DataCodec.encode(message, value)
    return message


def decode_response(message):
    message.next_frame()
    return CodecUtil.decode_nullable(message, DataCodec.decode)
