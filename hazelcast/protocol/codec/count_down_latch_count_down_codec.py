from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.codec.custom.raft_group_id_codec import RaftGroupIdCodec
from hazelcast.protocol.builtin import StringCodec

# hex: 0x0B0300
_REQUEST_MESSAGE_TYPE = 721664
# hex: 0x0B0301
_RESPONSE_MESSAGE_TYPE = 721665

_REQUEST_INVOCATION_UID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_EXPECTED_ROUND_OFFSET = _REQUEST_INVOCATION_UID_OFFSET + UUID_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_EXPECTED_ROUND_OFFSET + INT_SIZE_IN_BYTES


def encode_request(group_id, name, invocation_uid, expected_round):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_INVOCATION_UID_OFFSET, invocation_uid)
    FixSizedTypesCodec.encode_int(buf, _REQUEST_EXPECTED_ROUND_OFFSET, expected_round)
    RaftGroupIdCodec.encode(message, group_id)
    StringCodec.encode(message, name)
    return message
