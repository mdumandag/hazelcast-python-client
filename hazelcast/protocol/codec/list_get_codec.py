from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x050F00
_REQUEST_MESSAGE_TYPE = 331520
# hex: 0x050F01
_RESPONSE_MESSAGE_TYPE = 331521

_REQUEST_INDEX_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_INDEX_OFFSET + INT_SIZE_IN_BYTES


def encode_request(name, index):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_int(buf, _REQUEST_INDEX_OFFSET, index)
    StringCodec.encode(message, name)
    return message


def decode_response(message):
    message.next_frame()
    return CodecUtil.decode_nullable(message, DataCodec.decode)
