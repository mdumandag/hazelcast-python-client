from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec

# hex: 0x014100
_REQUEST_MESSAGE_TYPE = 82176
# hex: 0x014101
_RESPONSE_MESSAGE_TYPE = 82177

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE
_RESPONSE_OLDEST_SEQUENCE_OFFSET = RESPONSE_HEADER_SIZE
_RESPONSE_NEWEST_SEQUENCE_OFFSET = _RESPONSE_OLDEST_SEQUENCE_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(name):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    StringCodec.encode(message, name)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    response = dict()
    buf = initial_frame.buf
    response["oldest_sequence"] = FixSizedTypesCodec.decode_long(buf, _RESPONSE_OLDEST_SEQUENCE_OFFSET)
    response["newest_sequence"] = FixSizedTypesCodec.decode_long(buf, _RESPONSE_NEWEST_SEQUENCE_OFFSET)
    return response
