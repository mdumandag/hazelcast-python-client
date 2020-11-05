from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x170600
_REQUEST_MESSAGE_TYPE = 1508864
# hex: 0x170601
_RESPONSE_MESSAGE_TYPE = 1508865

_REQUEST_OVERFLOW_POLICY_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_OVERFLOW_POLICY_OFFSET + INT_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE


def encode_request(name, overflow_policy, value):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_int(buf, _REQUEST_OVERFLOW_POLICY_OFFSET, overflow_policy)
    StringCodec.encode(message, name)
    DataCodec.encode(message, value)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    return FixSizedTypesCodec.decode_long(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)
