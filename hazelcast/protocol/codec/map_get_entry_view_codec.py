from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.codec.custom.simple_entry_view_codec import SimpleEntryViewCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x011D00
_REQUEST_MESSAGE_TYPE = 72960
# hex: 0x011D01
_RESPONSE_MESSAGE_TYPE = 72961

_REQUEST_THREAD_ID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_THREAD_ID_OFFSET + LONG_SIZE_IN_BYTES
_RESPONSE_MAX_IDLE_OFFSET = RESPONSE_HEADER_SIZE


def encode_request(name, key, thread_id):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_long(buf, _REQUEST_THREAD_ID_OFFSET, thread_id)
    StringCodec.encode(message, name)
    DataCodec.encode(message, key)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    response = dict()
    buf = initial_frame.buf
    response["max_idle"] = FixSizedTypesCodec.decode_long(buf, _RESPONSE_MAX_IDLE_OFFSET)
    response["response"] = CodecUtil.decode_nullable(message, SimpleEntryViewCodec.decode)
    return response
