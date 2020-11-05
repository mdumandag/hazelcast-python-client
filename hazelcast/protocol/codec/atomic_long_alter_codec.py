from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.codec.custom.raft_group_id_codec import RaftGroupIdCodec
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x090200
_REQUEST_MESSAGE_TYPE = 590336
# hex: 0x090201
_RESPONSE_MESSAGE_TYPE = 590337

_REQUEST_RETURN_VALUE_TYPE_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_RETURN_VALUE_TYPE_OFFSET + INT_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE


def encode_request(group_id, name, function, return_value_type):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_int(buf, _REQUEST_RETURN_VALUE_TYPE_OFFSET, return_value_type)
    RaftGroupIdCodec.encode(message, group_id)
    StringCodec.encode(message, name)
    DataCodec.encode(message, function)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    return FixSizedTypesCodec.decode_long(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)
