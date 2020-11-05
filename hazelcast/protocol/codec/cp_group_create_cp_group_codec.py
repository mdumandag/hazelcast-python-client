from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.codec.custom.raft_group_id_codec import RaftGroupIdCodec

# hex: 0x1E0100
_REQUEST_MESSAGE_TYPE = 1966336
# hex: 0x1E0101
_RESPONSE_MESSAGE_TYPE = 1966337

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(proxy_name):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    StringCodec.encode(message, proxy_name)
    return message


def decode_response(message):
    message.next_frame()
    return RaftGroupIdCodec.decode(message)
