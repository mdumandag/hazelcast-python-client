from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import EntryListUUIDLongCodec

# hex: 0x1D0100
_REQUEST_MESSAGE_TYPE = 1900800
# hex: 0x1D0101
_RESPONSE_MESSAGE_TYPE = 1900801

_REQUEST_TARGET_REPLICA_UUID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_TARGET_REPLICA_UUID_OFFSET + UUID_SIZE_IN_BYTES
_RESPONSE_VALUE_OFFSET = RESPONSE_HEADER_SIZE
_RESPONSE_REPLICA_COUNT_OFFSET = _RESPONSE_VALUE_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(name, replica_timestamps, target_replica_uuid):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_TARGET_REPLICA_UUID_OFFSET, target_replica_uuid)
    StringCodec.encode(message, name)
    EntryListUUIDLongCodec.encode(message, replica_timestamps)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    response = dict()
    buf = initial_frame.buf
    response["value"] = FixSizedTypesCodec.decode_long(buf, _RESPONSE_VALUE_OFFSET)
    response["replica_count"] = FixSizedTypesCodec.decode_int(buf, _RESPONSE_REPLICA_COUNT_OFFSET)
    response["replica_timestamps"] = EntryListUUIDLongCodec.decode(message)
    return response
