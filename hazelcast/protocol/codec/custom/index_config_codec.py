from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME, create_initial_frame_custom, BEGIN_FRAME
from hazelcast.config import IndexConfig
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.bitmap_index_options_codec import BitmapIndexOptionsCodec


_TYPE_OFFSET = 0
_INITIAL_FRAME_SIZE = _TYPE_OFFSET + INT_SIZE_IN_BYTES


class IndexConfigCodec(object):
    @staticmethod
    def encode(message, index_config):
        message.add_frame(BEGIN_FRAME.copy())
        initial_frame = create_initial_frame_custom(_INITIAL_FRAME_SIZE)
        buf = initial_frame.buf
        FixSizedTypesCodec.encode_int(buf, _TYPE_OFFSET, index_config.type)
        message.add_frame(initial_frame)
        CodecUtil.encode_nullable(message, index_config.name, StringCodec.encode)
        ListMultiFrameCodec.encode(message, index_config.attributes, StringCodec.encode)
        CodecUtil.encode_nullable(message, index_config.bitmap_index_options, BitmapIndexOptionsCodec.encode)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def decode(message):
        message.next_frame()
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        type = FixSizedTypesCodec.decode_int(buf, _TYPE_OFFSET)
        name = CodecUtil.decode_nullable(message, StringCodec.decode)
        attributes = ListMultiFrameCodec.decode(message, StringCodec.decode)
        bitmap_index_options = CodecUtil.decode_nullable(message, BitmapIndexOptionsCodec.decode)
        CodecUtil.fast_forward_to_end_frame(message)
        return IndexConfig(name, type, attributes, bitmap_index_options)
