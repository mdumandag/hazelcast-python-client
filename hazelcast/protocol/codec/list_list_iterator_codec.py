from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x051700
_REQUEST_MESSAGE_TYPE = 333568
# hex: 0x051701
_RESPONSE_MESSAGE_TYPE = 333569

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
    return ListMultiFrameCodec.decode(message, DataCodec.decode)
