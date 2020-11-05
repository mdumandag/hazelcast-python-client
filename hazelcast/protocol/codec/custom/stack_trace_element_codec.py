from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME, create_initial_frame_custom, BEGIN_FRAME
from hazelcast.protocol import StackTraceElement
from hazelcast.protocol.builtin import StringCodec


_LINE_NUMBER_OFFSET = 0
_INITIAL_FRAME_SIZE = _LINE_NUMBER_OFFSET + INT_SIZE_IN_BYTES


class StackTraceElementCodec(object):
    @staticmethod
    def encode(message, stack_trace_element):
        message.add_frame(BEGIN_FRAME.copy())
        initial_frame = create_initial_frame_custom(_INITIAL_FRAME_SIZE)
        buf = initial_frame.buf
        FixSizedTypesCodec.encode_int(buf, _LINE_NUMBER_OFFSET, stack_trace_element.line_number)
        message.add_frame(initial_frame)
        StringCodec.encode(message, stack_trace_element.class_name)
        StringCodec.encode(message, stack_trace_element.method_name)
        CodecUtil.encode_nullable(message, stack_trace_element.file_name, StringCodec.encode)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def decode(message):
        message.next_frame()
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        line_number = FixSizedTypesCodec.decode_int(buf, _LINE_NUMBER_OFFSET)
        class_name = StringCodec.decode(message)
        method_name = StringCodec.decode(message)
        file_name = CodecUtil.decode_nullable(message, StringCodec.decode)
        CodecUtil.fast_forward_to_end_frame(message)
        return StackTraceElement(class_name, method_name, file_name, line_number)
