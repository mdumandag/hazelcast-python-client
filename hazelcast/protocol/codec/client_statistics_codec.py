from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ByteArrayCodec

# hex: 0x000C00
_REQUEST_MESSAGE_TYPE = 3072
# hex: 0x000C01
_RESPONSE_MESSAGE_TYPE = 3073

_REQUEST_TIMESTAMP_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_TIMESTAMP_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(timestamp, client_attributes, metrics_blob):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_long(buf, _REQUEST_TIMESTAMP_OFFSET, timestamp)
    StringCodec.encode(message, client_attributes)
    ByteArrayCodec.encode(message, metrics_blob)
    return message
