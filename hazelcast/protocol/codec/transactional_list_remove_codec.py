from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x110200
_REQUEST_MESSAGE_TYPE = 1114624
# hex: 0x110201
_RESPONSE_MESSAGE_TYPE = 1114625

_REQUEST_TXN_ID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_THREAD_ID_OFFSET = _REQUEST_TXN_ID_OFFSET + UUID_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_THREAD_ID_OFFSET + LONG_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE


def encode_request(name, txn_id, thread_id, item):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_TXN_ID_OFFSET, txn_id)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_THREAD_ID_OFFSET, thread_id)
    StringCodec.encode(message, name)
    DataCodec.encode(message, item)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    return FixSizedTypesCodec.decode_boolean(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)
