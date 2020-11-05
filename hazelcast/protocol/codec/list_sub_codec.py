from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x051500
_REQUEST_MESSAGE_TYPE = 333056
# hex: 0x051501
_RESPONSE_MESSAGE_TYPE = 333057

_REQUEST_FROM_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_TO_OFFSET = _REQUEST_FROM_OFFSET + INT_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_TO_OFFSET + INT_SIZE_IN_BYTES


def encode_request(name, _from, to):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_int(buf, _REQUEST_FROM_OFFSET, _from)
    FixSizedTypesCodec.encode_int(buf, _REQUEST_TO_OFFSET, to)
    StringCodec.encode(message, name)
    return message


def decode_response(message):
    message.next_frame()
    return ListMultiFrameCodec.decode(message, DataCodec.decode)
