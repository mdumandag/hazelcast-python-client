from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x010D00
_REQUEST_MESSAGE_TYPE = 68864
# hex: 0x010D01
_RESPONSE_MESSAGE_TYPE = 68865

_REQUEST_THREAD_ID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_TTL_OFFSET = _REQUEST_THREAD_ID_OFFSET + LONG_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_TTL_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(name, key, value, thread_id, ttl):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_long(buf, _REQUEST_THREAD_ID_OFFSET, thread_id)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_TTL_OFFSET, ttl)
    StringCodec.encode(message, name)
    DataCodec.encode(message, key)
    DataCodec.encode(message, value)
    return message
