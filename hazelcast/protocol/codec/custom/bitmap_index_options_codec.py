from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME, create_initial_frame_custom, BEGIN_FRAME
from hazelcast.config import BitmapIndexOptions
from hazelcast.protocol.builtin import StringCodec


_UNIQUE_KEY_TRANSFORMATION_OFFSET = 0
_INITIAL_FRAME_SIZE = _UNIQUE_KEY_TRANSFORMATION_OFFSET + INT_SIZE_IN_BYTES


class BitmapIndexOptionsCodec(object):
    @staticmethod
    def encode(message, bitmap_index_options):
        message.add_frame(BEGIN_FRAME.copy())
        initial_frame = create_initial_frame_custom(_INITIAL_FRAME_SIZE)
        buf = initial_frame.buf
        FixSizedTypesCodec.encode_int(buf, _UNIQUE_KEY_TRANSFORMATION_OFFSET, bitmap_index_options.unique_key_transformation)
        message.add_frame(initial_frame)
        StringCodec.encode(message, bitmap_index_options.unique_key)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def decode(message):
        message.next_frame()
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        unique_key_transformation = FixSizedTypesCodec.decode_int(buf, _UNIQUE_KEY_TRANSFORMATION_OFFSET)
        unique_key = StringCodec.decode(message)
        CodecUtil.fast_forward_to_end_frame(message)
        return BitmapIndexOptions(unique_key, unique_key_transformation)
