from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.codec.custom.raft_group_id_codec import RaftGroupIdCodec
from hazelcast.protocol.builtin import StringCodec

# hex: 0x0C0400
_REQUEST_MESSAGE_TYPE = 787456
# hex: 0x0C0401
_RESPONSE_MESSAGE_TYPE = 787457

_REQUEST_SESSION_ID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_THREAD_ID_OFFSET = _REQUEST_SESSION_ID_OFFSET + LONG_SIZE_IN_BYTES
_REQUEST_INVOCATION_UID_OFFSET = _REQUEST_THREAD_ID_OFFSET + LONG_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_INVOCATION_UID_OFFSET + UUID_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE


def encode_request(group_id, name, session_id, thread_id, invocation_uid):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_long(buf, _REQUEST_SESSION_ID_OFFSET, session_id)
    FixSizedTypesCodec.encode_long(buf, _REQUEST_THREAD_ID_OFFSET, thread_id)
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_INVOCATION_UID_OFFSET, invocation_uid)
    RaftGroupIdCodec.encode(message, group_id)
    StringCodec.encode(message, name)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    return FixSizedTypesCodec.decode_int(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)
