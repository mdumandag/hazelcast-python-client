from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE, EVENT_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import CodecUtil

# hex: 0x060B00
_REQUEST_MESSAGE_TYPE = 396032
# hex: 0x060B01
_RESPONSE_MESSAGE_TYPE = 396033
# hex: 0x060B02
_EVENT_ITEM_MESSAGE_TYPE = 396034

_REQUEST_INCLUDE_VALUE_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_LOCAL_ONLY_OFFSET = _REQUEST_INCLUDE_VALUE_OFFSET + BOOLEAN_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_LOCAL_ONLY_OFFSET + BOOLEAN_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE
_EVENT_ITEM_UUID_OFFSET = EVENT_HEADER_SIZE
_EVENT_ITEM_EVENT_TYPE_OFFSET = _EVENT_ITEM_UUID_OFFSET + UUID_SIZE_IN_BYTES


def encode_request(name, include_value, local_only):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_INCLUDE_VALUE_OFFSET, include_value)
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_LOCAL_ONLY_OFFSET, local_only)
    StringCodec.encode(message, name)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    return FixSizedTypesCodec.decode_uuid(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)


def handle(message, handle_item_event=None):
    message_type = message.get_message_type()
    if message_type == _EVENT_ITEM_MESSAGE_TYPE and handle_item_event is not None:
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        uuid = FixSizedTypesCodec.decode_uuid(buf, _EVENT_ITEM_UUID_OFFSET)
        event_type = FixSizedTypesCodec.decode_int(buf, _EVENT_ITEM_EVENT_TYPE_OFFSET)
        item = CodecUtil.decode_nullable(message, DataCodec.decode)
        handle_item_event(item, uuid, event_type)
        return
