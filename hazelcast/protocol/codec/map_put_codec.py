from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x010100
_REQUEST_MESSAGE_TYPE = 65792
# hex: 0x010101
_RESPONSE_MESSAGE_TYPE = 65793

_REQUEST_THREAD_ID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_TTL_OFFSET = _REQUEST_THREAD_ID_OFFSET + LONG_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_TTL_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(name, key, value, thread_id, ttl):
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_THREAD_ID_OFFSET, thread_id)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_TTL_OFFSET, ttl)
    StringCodec.encode(buf, name)
    DataCodec.encode(buf, key)
    DataCodec.encode(buf, value, True)
    return OutboundMessage(buf, False)


def decode_response(msg):
    msg.next_frame()
    return CodecUtil.decode_nullable(msg, DataCodec.decode)
