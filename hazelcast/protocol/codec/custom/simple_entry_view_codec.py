from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME, create_initial_frame_custom, BEGIN_FRAME
from hazelcast.core import SimpleEntryView
from hazelcast.protocol.builtin import DataCodec


_COST_OFFSET = 0
_CREATION_TIME_OFFSET = _COST_OFFSET + LONG_SIZE_IN_BYTES
_EXPIRATION_TIME_OFFSET = _CREATION_TIME_OFFSET + LONG_SIZE_IN_BYTES
_HITS_OFFSET = _EXPIRATION_TIME_OFFSET + LONG_SIZE_IN_BYTES
_LAST_ACCESS_TIME_OFFSET = _HITS_OFFSET + LONG_SIZE_IN_BYTES
_LAST_STORED_TIME_OFFSET = _LAST_ACCESS_TIME_OFFSET + LONG_SIZE_IN_BYTES
_LAST_UPDATE_TIME_OFFSET = _LAST_STORED_TIME_OFFSET + LONG_SIZE_IN_BYTES
_VERSION_OFFSET = _LAST_UPDATE_TIME_OFFSET + LONG_SIZE_IN_BYTES
_TTL_OFFSET = _VERSION_OFFSET + LONG_SIZE_IN_BYTES
_MAX_IDLE_OFFSET = _TTL_OFFSET + LONG_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _MAX_IDLE_OFFSET + LONG_SIZE_IN_BYTES


class SimpleEntryViewCodec(object):
    @staticmethod
    def encode(message, simple_entry_view):
        message.add_frame(BEGIN_FRAME.copy())
        initial_frame = create_initial_frame_custom(_INITIAL_FRAME_SIZE)
        buf = initial_frame.buf
        FixSizedTypesCodec.encode_long(buf, _COST_OFFSET, simple_entry_view.cost)
        FixSizedTypesCodec.encode_long(buf, _CREATION_TIME_OFFSET, simple_entry_view.creation_time)
        FixSizedTypesCodec.encode_long(buf, _EXPIRATION_TIME_OFFSET, simple_entry_view.expiration_time)
        FixSizedTypesCodec.encode_long(buf, _HITS_OFFSET, simple_entry_view.hits)
        FixSizedTypesCodec.encode_long(buf, _LAST_ACCESS_TIME_OFFSET, simple_entry_view.last_access_time)
        FixSizedTypesCodec.encode_long(buf, _LAST_STORED_TIME_OFFSET, simple_entry_view.last_stored_time)
        FixSizedTypesCodec.encode_long(buf, _LAST_UPDATE_TIME_OFFSET, simple_entry_view.last_update_time)
        FixSizedTypesCodec.encode_long(buf, _VERSION_OFFSET, simple_entry_view.version)
        FixSizedTypesCodec.encode_long(buf, _TTL_OFFSET, simple_entry_view.ttl)
        FixSizedTypesCodec.encode_long(buf, _MAX_IDLE_OFFSET, simple_entry_view.max_idle)
        message.add_frame(initial_frame)
        DataCodec.encode(message, simple_entry_view.key)
        DataCodec.encode(message, simple_entry_view.value)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def decode(message):
        message.next_frame()
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        cost = FixSizedTypesCodec.decode_long(buf, _COST_OFFSET)
        creation_time = FixSizedTypesCodec.decode_long(buf, _CREATION_TIME_OFFSET)
        expiration_time = FixSizedTypesCodec.decode_long(buf, _EXPIRATION_TIME_OFFSET)
        hits = FixSizedTypesCodec.decode_long(buf, _HITS_OFFSET)
        last_access_time = FixSizedTypesCodec.decode_long(buf, _LAST_ACCESS_TIME_OFFSET)
        last_stored_time = FixSizedTypesCodec.decode_long(buf, _LAST_STORED_TIME_OFFSET)
        last_update_time = FixSizedTypesCodec.decode_long(buf, _LAST_UPDATE_TIME_OFFSET)
        version = FixSizedTypesCodec.decode_long(buf, _VERSION_OFFSET)
        ttl = FixSizedTypesCodec.decode_long(buf, _TTL_OFFSET)
        max_idle = FixSizedTypesCodec.decode_long(buf, _MAX_IDLE_OFFSET)
        key = DataCodec.decode(message)
        value = DataCodec.decode(message)
        CodecUtil.fast_forward_to_end_frame(message)
        return SimpleEntryView(key, value, cost, creation_time, expiration_time, hits, last_access_time, last_stored_time, last_update_time, version, ttl, max_idle)
