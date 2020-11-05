from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME, create_initial_frame_custom, BEGIN_FRAME
from hazelcast.protocol import ErrorHolder
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.stack_trace_element_codec import StackTraceElementCodec


_ERROR_CODE_OFFSET = 0
_INITIAL_FRAME_SIZE = _ERROR_CODE_OFFSET + INT_SIZE_IN_BYTES


class ErrorHolderCodec(object):
    @staticmethod
    def encode(message, error_holder):
        message.add_frame(BEGIN_FRAME.copy())
        initial_frame = create_initial_frame_custom(_INITIAL_FRAME_SIZE)
        buf = initial_frame.buf
        FixSizedTypesCodec.encode_int(buf, _ERROR_CODE_OFFSET, error_holder.error_code)
        message.add_frame(initial_frame)
        StringCodec.encode(message, error_holder.class_name)
        CodecUtil.encode_nullable(message, error_holder.message, StringCodec.encode)
        ListMultiFrameCodec.encode(message, error_holder.stack_trace_elements, StackTraceElementCodec.encode)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def decode(message):
        message.next_frame()
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        error_code = FixSizedTypesCodec.decode_int(buf, _ERROR_CODE_OFFSET)
        class_name = StringCodec.decode(message)
        message = CodecUtil.decode_nullable(message, StringCodec.decode)
        stack_trace_elements = ListMultiFrameCodec.decode(message, StackTraceElementCodec.decode)
        CodecUtil.fast_forward_to_end_frame(message)
        return ErrorHolder(error_code, class_name, message, stack_trace_elements)
