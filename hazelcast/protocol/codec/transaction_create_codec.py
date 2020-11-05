from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE

# hex: 0x150200
_REQUEST_MESSAGE_TYPE = 1376768
# hex: 0x150201
_RESPONSE_MESSAGE_TYPE = 1376769

_REQUEST_TIMEOUT_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_DURABILITY_OFFSET = _REQUEST_TIMEOUT_OFFSET + LONG_SIZE_IN_BYTES
_REQUEST_TRANSACTION_TYPE_OFFSET = _REQUEST_DURABILITY_OFFSET + INT_SIZE_IN_BYTES
_REQUEST_THREAD_ID_OFFSET = _REQUEST_TRANSACTION_TYPE_OFFSET + INT_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_THREAD_ID_OFFSET + LONG_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE


def encode_request(timeout, durability, transaction_type, thread_id):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_long(buf, _REQUEST_TIMEOUT_OFFSET, timeout)
    FixSizedTypesCodec.encode_int(buf, _REQUEST_DURABILITY_OFFSET, durability)
    FixSizedTypesCodec.encode_int(buf, _REQUEST_TRANSACTION_TYPE_OFFSET, transaction_type)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_THREAD_ID_OFFSET, thread_id)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    return FixSizedTypesCodec.decode_uuid(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)
