from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.codec.custom.raft_group_id_codec import RaftGroupIdCodec
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x0A0200
_REQUEST_MESSAGE_TYPE = 655872
# hex: 0x0A0201
_RESPONSE_MESSAGE_TYPE = 655873

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE


def encode_request(group_id, name, old_value, new_value):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    RaftGroupIdCodec.encode(message, group_id)
    StringCodec.encode(message, name)
    CodecUtil.encode_nullable(message, old_value, DataCodec.encode)
    CodecUtil.encode_nullable(message, new_value, DataCodec.encode)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    return FixSizedTypesCodec.decode_boolean(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)
