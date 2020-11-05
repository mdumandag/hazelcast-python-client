from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec

# hex: 0x020F00
_REQUEST_MESSAGE_TYPE = 134912
# hex: 0x020F01
_RESPONSE_MESSAGE_TYPE = 134913

_REQUEST_REGISTRATION_ID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_REGISTRATION_ID_OFFSET + UUID_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE


def encode_request(name, registration_id):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_REGISTRATION_ID_OFFSET, registration_id)
    StringCodec.encode(message, name)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    return FixSizedTypesCodec.decode_boolean(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)
