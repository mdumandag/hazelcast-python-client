from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec

# hex: 0x1C0100
_REQUEST_MESSAGE_TYPE = 1835264
# hex: 0x1C0101
_RESPONSE_MESSAGE_TYPE = 1835265

_REQUEST_BATCH_SIZE_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_BATCH_SIZE_OFFSET + INT_SIZE_IN_BYTES
_RESPONSE_BASE_OFFSET = RESPONSE_HEADER_SIZE
_RESPONSE_INCREMENT_OFFSET = _RESPONSE_BASE_OFFSET + LONG_SIZE_IN_BYTES
_RESPONSE_BATCH_SIZE_OFFSET = _RESPONSE_INCREMENT_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(name, batch_size):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_int(buf, _REQUEST_BATCH_SIZE_OFFSET, batch_size)
    StringCodec.encode(message, name)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    response = dict()
    buf = initial_frame.buf
    response["base"] = FixSizedTypesCodec.decode_long(buf, _RESPONSE_BASE_OFFSET)
    response["increment"] = FixSizedTypesCodec.decode_long(buf, _RESPONSE_INCREMENT_OFFSET)
    response["batch_size"] = FixSizedTypesCodec.decode_int(buf, _RESPONSE_BATCH_SIZE_OFFSET)
    return response
