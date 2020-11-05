from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x021100
_REQUEST_MESSAGE_TYPE = 135424
# hex: 0x021101
_RESPONSE_MESSAGE_TYPE = 135425

_REQUEST_THREAD_ID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_LEASE_OFFSET = _REQUEST_THREAD_ID_OFFSET + LONG_SIZE_IN_BYTES
_REQUEST_TIMEOUT_OFFSET = _REQUEST_LEASE_OFFSET + LONG_SIZE_IN_BYTES
_REQUEST_REFERENCE_ID_OFFSET = _REQUEST_TIMEOUT_OFFSET + LONG_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_REFERENCE_ID_OFFSET + LONG_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE


def encode_request(name, key, thread_id, lease, timeout, reference_id):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_long(buf, _REQUEST_THREAD_ID_OFFSET, thread_id)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_LEASE_OFFSET, lease)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_TIMEOUT_OFFSET, timeout)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_REFERENCE_ID_OFFSET, reference_id)
    StringCodec.encode(message, name)
    DataCodec.encode(message, key)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    return FixSizedTypesCodec.decode_boolean(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)
