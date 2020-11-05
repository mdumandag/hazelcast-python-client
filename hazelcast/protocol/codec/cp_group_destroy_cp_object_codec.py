from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.codec.custom.raft_group_id_codec import RaftGroupIdCodec
from hazelcast.protocol.builtin import StringCodec

# hex: 0x1E0200
_REQUEST_MESSAGE_TYPE = 1966592
# hex: 0x1E0201
_RESPONSE_MESSAGE_TYPE = 1966593

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(group_id, service_name, object_name):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    RaftGroupIdCodec.encode(message, group_id)
    StringCodec.encode(message, service_name)
    StringCodec.encode(message, object_name)
    return message
