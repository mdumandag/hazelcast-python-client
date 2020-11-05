from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec

# hex: 0x1D0300
_REQUEST_MESSAGE_TYPE = 1901312
# hex: 0x1D0301
_RESPONSE_MESSAGE_TYPE = 1901313

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE


def encode_request(name):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    StringCodec.encode(message, name)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    return FixSizedTypesCodec.decode_int(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)
