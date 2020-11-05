from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME, create_initial_frame_custom, BEGIN_FRAME
from hazelcast.core import Address
from hazelcast.protocol.builtin import StringCodec


_PORT_OFFSET = 0
_INITIAL_FRAME_SIZE = _PORT_OFFSET + INT_SIZE_IN_BYTES


class AddressCodec(object):
    @staticmethod
    def encode(message, address):
        message.add_frame(BEGIN_FRAME.copy())
        initial_frame = create_initial_frame_custom(_INITIAL_FRAME_SIZE)
        buf = initial_frame.buf
        FixSizedTypesCodec.encode_int(buf, _PORT_OFFSET, address.port)
        message.add_frame(initial_frame)
        StringCodec.encode(message, address.host)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def decode(message):
        message.next_frame()
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        port = FixSizedTypesCodec.decode_int(buf, _PORT_OFFSET)
        host = StringCodec.decode(message)
        CodecUtil.fast_forward_to_end_frame(message)
        return Address(host, port)
