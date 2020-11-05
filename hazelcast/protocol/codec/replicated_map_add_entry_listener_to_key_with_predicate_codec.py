from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE, EVENT_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x0D0A00
_REQUEST_MESSAGE_TYPE = 854528
# hex: 0x0D0A01
_RESPONSE_MESSAGE_TYPE = 854529
# hex: 0x0D0A02
_EVENT_ENTRY_MESSAGE_TYPE = 854530

_REQUEST_LOCAL_ONLY_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_LOCAL_ONLY_OFFSET + BOOLEAN_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE
_EVENT_ENTRY_EVENT_TYPE_OFFSET = EVENT_HEADER_SIZE
_EVENT_ENTRY_UUID_OFFSET = _EVENT_ENTRY_EVENT_TYPE_OFFSET + INT_SIZE_IN_BYTES
_EVENT_ENTRY_NUMBER_OF_AFFECTED_ENTRIES_OFFSET = _EVENT_ENTRY_UUID_OFFSET + UUID_SIZE_IN_BYTES


def encode_request(name, key, predicate, local_only):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_LOCAL_ONLY_OFFSET, local_only)
    StringCodec.encode(message, name)
    DataCodec.encode(message, key)
    DataCodec.encode(message, predicate)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    return FixSizedTypesCodec.decode_uuid(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)


def handle(message, handle_entry_event=None):
    message_type = message.get_message_type()
    if message_type == _EVENT_ENTRY_MESSAGE_TYPE and handle_entry_event is not None:
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        event_type = FixSizedTypesCodec.decode_int(buf, _EVENT_ENTRY_EVENT_TYPE_OFFSET)
        uuid = FixSizedTypesCodec.decode_uuid(buf, _EVENT_ENTRY_UUID_OFFSET)
        number_of_affected_entries = FixSizedTypesCodec.decode_int(buf, _EVENT_ENTRY_NUMBER_OF_AFFECTED_ENTRIES_OFFSET)
        key = CodecUtil.decode_nullable(message, DataCodec.decode)
        value = CodecUtil.decode_nullable(message, DataCodec.decode)
        old_value = CodecUtil.decode_nullable(message, DataCodec.decode)
        merging_value = CodecUtil.decode_nullable(message, DataCodec.decode)
        handle_entry_event(key, value, old_value, merging_value, event_type, uuid, number_of_affected_entries)
        return
