from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE, EVENT_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import ListUUIDCodec
from hazelcast.protocol.builtin import ListLongCodec

# hex: 0x013F00
_REQUEST_MESSAGE_TYPE = 81664
# hex: 0x013F01
_RESPONSE_MESSAGE_TYPE = 81665
# hex: 0x013F02
_EVENT_I_MAP_INVALIDATION_MESSAGE_TYPE = 81666
# hex: 0x013F03
_EVENT_I_MAP_BATCH_INVALIDATION_MESSAGE_TYPE = 81667

_REQUEST_LISTENER_FLAGS_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_LOCAL_ONLY_OFFSET = _REQUEST_LISTENER_FLAGS_OFFSET + INT_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_LOCAL_ONLY_OFFSET + BOOLEAN_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE
_EVENT_I_MAP_INVALIDATION_SOURCE_UUID_OFFSET = EVENT_HEADER_SIZE
_EVENT_I_MAP_INVALIDATION_PARTITION_UUID_OFFSET = _EVENT_I_MAP_INVALIDATION_SOURCE_UUID_OFFSET + UUID_SIZE_IN_BYTES
_EVENT_I_MAP_INVALIDATION_SEQUENCE_OFFSET = _EVENT_I_MAP_INVALIDATION_PARTITION_UUID_OFFSET + UUID_SIZE_IN_BYTES


def encode_request(name, listener_flags, local_only):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_int(buf, _REQUEST_LISTENER_FLAGS_OFFSET, listener_flags)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_LOCAL_ONLY_OFFSET, local_only)
    StringCodec.encode(message, name)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    return FixSizedTypesCodec.decode_uuid(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)


def handle(message, handle_i_map_invalidation_event=None, handle_i_map_batch_invalidation_event=None):
    message_type = message.get_message_type()
    if message_type == _EVENT_I_MAP_INVALIDATION_MESSAGE_TYPE and handle_i_map_invalidation_event is not None:
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        source_uuid = FixSizedTypesCodec.decode_uuid(buf, _EVENT_I_MAP_INVALIDATION_SOURCE_UUID_OFFSET)
        partition_uuid = FixSizedTypesCodec.decode_uuid(buf, _EVENT_I_MAP_INVALIDATION_PARTITION_UUID_OFFSET)
        sequence = FixSizedTypesCodec.decode_long(buf, _EVENT_I_MAP_INVALIDATION_SEQUENCE_OFFSET)
        key = CodecUtil.decode_nullable(message, DataCodec.decode)
        handle_i_map_invalidation_event(key, source_uuid, partition_uuid, sequence)
        return
    if message_type == _EVENT_I_MAP_BATCH_INVALIDATION_MESSAGE_TYPE and handle_i_map_batch_invalidation_event is not None:
        message.next_frame()
        keys = ListMultiFrameCodec.decode(message, DataCodec.decode)
        source_uuids = ListUUIDCodec.decode(message)
        partition_uuids = ListUUIDCodec.decode(message)
        sequences = ListLongCodec.decode(message)
        handle_i_map_batch_invalidation_event(keys, source_uuids, partition_uuids, sequences)
        return
