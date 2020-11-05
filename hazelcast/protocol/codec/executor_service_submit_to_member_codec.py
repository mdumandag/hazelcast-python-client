from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x080600
_REQUEST_MESSAGE_TYPE = 525824
# hex: 0x080601
_RESPONSE_MESSAGE_TYPE = 525825

_REQUEST_UUID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_MEMBER_UUID_OFFSET = _REQUEST_UUID_OFFSET + UUID_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_MEMBER_UUID_OFFSET + UUID_SIZE_IN_BYTES


def encode_request(name, uuid, callable, member_uuid):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_UUID_OFFSET, uuid)
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_MEMBER_UUID_OFFSET, member_uuid)
    StringCodec.encode(message, name)
    DataCodec.encode(message, callable)
    return message


def decode_response(message):
    message.next_frame()
    return CodecUtil.decode_nullable(message, DataCodec.decode)
