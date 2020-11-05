from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME, create_initial_frame_custom, BEGIN_FRAME
from hazelcast.protocol import RaftGroupId
from hazelcast.protocol.builtin import StringCodec


_SEED_OFFSET = 0
_ID_OFFSET = _SEED_OFFSET + LONG_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _ID_OFFSET + LONG_SIZE_IN_BYTES


class RaftGroupIdCodec(object):
    @staticmethod
    def encode(message, raft_group_id):
        message.add_frame(BEGIN_FRAME.copy())
        initial_frame = create_initial_frame_custom(_INITIAL_FRAME_SIZE)
        buf = initial_frame.buf
        FixSizedTypesCodec.encode_long(buf, _SEED_OFFSET, raft_group_id.seed)
        FixSizedTypesCodec.encode_long(buf, _ID_OFFSET, raft_group_id.id)
        message.add_frame(initial_frame)
        StringCodec.encode(message, raft_group_id.name)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def decode(message):
        message.next_frame()
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        seed = FixSizedTypesCodec.decode_long(buf, _SEED_OFFSET)
        id = FixSizedTypesCodec.decode_long(buf, _ID_OFFSET)
        name = StringCodec.decode(message)
        CodecUtil.fast_forward_to_end_frame(message)
        return RaftGroupId(name, seed, id)
