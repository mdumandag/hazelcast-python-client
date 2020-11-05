from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME, create_initial_frame_custom, BEGIN_FRAME
from hazelcast.protocol import EndpointQualifier
from hazelcast.protocol.builtin import StringCodec


_TYPE_OFFSET = 0
_INITIAL_FRAME_SIZE = _TYPE_OFFSET + INT_SIZE_IN_BYTES


class EndpointQualifierCodec(object):
    @staticmethod
    def encode(message, endpoint_qualifier):
        message.add_frame(BEGIN_FRAME.copy())
        initial_frame = create_initial_frame_custom(_INITIAL_FRAME_SIZE)
        buf = initial_frame.buf
        FixSizedTypesCodec.encode_int(buf, _TYPE_OFFSET, endpoint_qualifier.type)
        message.add_frame(initial_frame)
        CodecUtil.encode_nullable(message, endpoint_qualifier.identifier, StringCodec.encode)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def decode(message):
        message.next_frame()
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        type = FixSizedTypesCodec.decode_int(buf, _TYPE_OFFSET)
        identifier = CodecUtil.decode_nullable(message, StringCodec.decode)
        CodecUtil.fast_forward_to_end_frame(message)
        return EndpointQualifier(type, identifier)
