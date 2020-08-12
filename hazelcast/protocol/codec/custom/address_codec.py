from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME_BUF, SIZE_OF_FRAME_LENGTH_AND_FLAGS, create_initial_buffer_custom
from hazelcast.core import Address
from hazelcast.protocol.builtin import StringCodec

_PORT_OFFSET = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS
_INITIAL_FRAME_SIZE = _PORT_OFFSET + INT_SIZE_IN_BYTES - 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS


class AddressCodec(object):
    @staticmethod
    def encode(buf, address):
        initial_frame_buf = create_initial_buffer_custom(_INITIAL_FRAME_SIZE, False)
        FixSizedTypesCodec.encode_int(initial_frame_buf, _PORT_OFFSET, address.port)
        buf.extend(initial_frame_buf)
        StringCodec.encode(buf, address.host)
        buf.extend(END_FRAME_BUF)

    @staticmethod
    def decode(msg):
        msg.next_frame()
        initial_frame = msg.next_frame()
        port = FixSizedTypesCodec.decode_int(initial_frame.buf, _PORT_OFFSET)
        host = StringCodec.decode(msg)
        CodecUtil.fast_forward_to_end_frame(msg)
        return Address(host, port)