from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import EntryListUUIDLongCodec

# hex: 0x1D0200
_REQUEST_MESSAGE_TYPE = 1901056
# hex: 0x1D0201
_RESPONSE_MESSAGE_TYPE = 1901057

_REQUEST_DELTA_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_GET_BEFORE_UPDATE_OFFSET = _REQUEST_DELTA_OFFSET + LONG_SIZE_IN_BYTES
_REQUEST_TARGET_REPLICA_UUID_OFFSET = _REQUEST_GET_BEFORE_UPDATE_OFFSET + BOOLEAN_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_TARGET_REPLICA_UUID_OFFSET + UUID_SIZE_IN_BYTES
_RESPONSE_VALUE_OFFSET = RESPONSE_HEADER_SIZE
_RESPONSE_REPLICA_COUNT_OFFSET = _RESPONSE_VALUE_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(name, delta, get_before_update, replica_timestamps, target_replica_uuid):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_long(buf, _REQUEST_DELTA_OFFSET, delta)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_GET_BEFORE_UPDATE_OFFSET, get_before_update)
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
