from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME, create_initial_frame_custom, BEGIN_FRAME
from hazelcast.core import MemberVersion


_MAJOR_OFFSET = 0
_MINOR_OFFSET = _MAJOR_OFFSET + BYTE_SIZE_IN_BYTES
_PATCH_OFFSET = _MINOR_OFFSET + BYTE_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _PATCH_OFFSET + BYTE_SIZE_IN_BYTES


class MemberVersionCodec(object):
    @staticmethod
    def encode(message, member_version):
        message.add_frame(BEGIN_FRAME.copy())
        initial_frame = create_initial_frame_custom(_INITIAL_FRAME_SIZE)
        buf = initial_frame.buf
        FixSizedTypesCodec.encode_byte(buf, _MAJOR_OFFSET, member_version.major)
        FixSizedTypesCodec.encode_byte(buf, _MINOR_OFFSET, member_version.minor)
        FixSizedTypesCodec.encode_byte(buf, _PATCH_OFFSET, member_version.patch)
        message.add_frame(initial_frame)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def decode(message):
        message.next_frame()
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        major = FixSizedTypesCodec.decode_byte(buf, _MAJOR_OFFSET)
        minor = FixSizedTypesCodec.decode_byte(buf, _MINOR_OFFSET)
        patch = FixSizedTypesCodec.decode_byte(buf, _PATCH_OFFSET)
        CodecUtil.fast_forward_to_end_frame(message)
        return MemberVersion(major, minor, patch)
