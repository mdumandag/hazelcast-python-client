from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x021300
_REQUEST_MESSAGE_TYPE = 135936
# hex: 0x021301
_RESPONSE_MESSAGE_TYPE = 135937

_REQUEST_THREAD_ID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_REFERENCE_ID_OFFSET = _REQUEST_THREAD_ID_OFFSET + LONG_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_REFERENCE_ID_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(name, key, thread_id, reference_id):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_long(buf, _REQUEST_THREAD_ID_OFFSET, thread_id)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_REFERENCE_ID_OFFSET, reference_id)
    StringCodec.encode(message, name)
    DataCodec.encode(message, key)
    return message
