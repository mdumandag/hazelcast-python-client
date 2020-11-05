from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame

# hex: 0x150100
_REQUEST_MESSAGE_TYPE = 1376512
# hex: 0x150101
_RESPONSE_MESSAGE_TYPE = 1376513

_REQUEST_TRANSACTION_ID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_THREAD_ID_OFFSET = _REQUEST_TRANSACTION_ID_OFFSET + UUID_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_THREAD_ID_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(transaction_id, thread_id):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_TRANSACTION_ID_OFFSET, transaction_id)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_THREAD_ID_OFFSET, thread_id)
    return message
