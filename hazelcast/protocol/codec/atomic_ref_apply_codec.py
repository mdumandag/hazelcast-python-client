from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.codec.custom.raft_group_id_codec import RaftGroupIdCodec
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x0A0100
_REQUEST_MESSAGE_TYPE = 655616
# hex: 0x0A0101
_RESPONSE_MESSAGE_TYPE = 655617

_REQUEST_RETURN_VALUE_TYPE_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_ALTER_OFFSET = _REQUEST_RETURN_VALUE_TYPE_OFFSET + INT_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_ALTER_OFFSET + BOOLEAN_SIZE_IN_BYTES


def encode_request(group_id, name, function, return_value_type, alter):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_int(buf, _REQUEST_RETURN_VALUE_TYPE_OFFSET, return_value_type)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_ALTER_OFFSET, alter)
    RaftGroupIdCodec.encode(message, group_id)
    StringCodec.encode(message, name)
    DataCodec.encode(message, function)
    return message


def decode_response(message):
    message.next_frame()
    return CodecUtil.decode_nullable(message, DataCodec.decode)
