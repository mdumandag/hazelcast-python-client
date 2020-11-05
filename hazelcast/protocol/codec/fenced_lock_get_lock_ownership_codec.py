from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.codec.custom.raft_group_id_codec import RaftGroupIdCodec
from hazelcast.protocol.builtin import StringCodec

# hex: 0x070400
_REQUEST_MESSAGE_TYPE = 459776
# hex: 0x070401
_RESPONSE_MESSAGE_TYPE = 459777

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE
_RESPONSE_FENCE_OFFSET = RESPONSE_HEADER_SIZE
_RESPONSE_LOCK_COUNT_OFFSET = _RESPONSE_FENCE_OFFSET + LONG_SIZE_IN_BYTES
_RESPONSE_SESSION_ID_OFFSET = _RESPONSE_LOCK_COUNT_OFFSET + INT_SIZE_IN_BYTES
_RESPONSE_THREAD_ID_OFFSET = _RESPONSE_SESSION_ID_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(group_id, name):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    RaftGroupIdCodec.encode(message, group_id)
    StringCodec.encode(message, name)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    response = dict()
    buf = initial_frame.buf
    response["fence"] = FixSizedTypesCodec.decode_long(buf, _RESPONSE_FENCE_OFFSET)
    response["lock_count"] = FixSizedTypesCodec.decode_int(buf, _RESPONSE_LOCK_COUNT_OFFSET)
    response["session_id"] = FixSizedTypesCodec.decode_long(buf, _RESPONSE_SESSION_ID_OFFSET)
    response["thread_id"] = FixSizedTypesCodec.decode_long(buf, _RESPONSE_THREAD_ID_OFFSET)
    return response
