from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import LongArrayCodec

# hex: 0x014200
_REQUEST_MESSAGE_TYPE = 82432
# hex: 0x014201
_RESPONSE_MESSAGE_TYPE = 82433

_REQUEST_START_SEQUENCE_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_MIN_SIZE_OFFSET = _REQUEST_START_SEQUENCE_OFFSET + LONG_SIZE_IN_BYTES
_REQUEST_MAX_SIZE_OFFSET = _REQUEST_MIN_SIZE_OFFSET + INT_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_MAX_SIZE_OFFSET + INT_SIZE_IN_BYTES
_RESPONSE_READ_COUNT_OFFSET = RESPONSE_HEADER_SIZE
_RESPONSE_NEXT_SEQ_OFFSET = _RESPONSE_READ_COUNT_OFFSET + INT_SIZE_IN_BYTES


def encode_request(name, start_sequence, min_size, max_size, predicate, projection):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_long(buf, _REQUEST_START_SEQUENCE_OFFSET, start_sequence)
    FixSizedTypesCodec.encode_int(buf, _REQUEST_MIN_SIZE_OFFSET, min_size)
    FixSizedTypesCodec.encode_int(buf, _REQUEST_MAX_SIZE_OFFSET, max_size)
    StringCodec.encode(message, name)
    CodecUtil.encode_nullable(message, predicate, DataCodec.encode)
    CodecUtil.encode_nullable(message, projection, DataCodec.encode)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    response = dict()
    buf = initial_frame.buf
    response["read_count"] = FixSizedTypesCodec.decode_int(buf, _RESPONSE_READ_COUNT_OFFSET)
    response["next_seq"] = FixSizedTypesCodec.decode_long(buf, _RESPONSE_NEXT_SEQ_OFFSET)
    response["items"] = ListMultiFrameCodec.decode(message, DataCodec.decode)
    response["item_seqs"] = CodecUtil.decode_nullable(message, LongArrayCodec.decode)
    return response
