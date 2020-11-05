from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE, EVENT_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x040200
_REQUEST_MESSAGE_TYPE = 262656
# hex: 0x040201
_RESPONSE_MESSAGE_TYPE = 262657
# hex: 0x040202
_EVENT_TOPIC_MESSAGE_TYPE = 262658

_REQUEST_LOCAL_ONLY_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_LOCAL_ONLY_OFFSET + BOOLEAN_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE
_EVENT_TOPIC_PUBLISH_TIME_OFFSET = EVENT_HEADER_SIZE
_EVENT_TOPIC_UUID_OFFSET = _EVENT_TOPIC_PUBLISH_TIME_OFFSET + LONG_SIZE_IN_BYTES


def encode_request(name, local_only):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_LOCAL_ONLY_OFFSET, local_only)
    StringCodec.encode(message, name)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    return FixSizedTypesCodec.decode_uuid(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)


def handle(message, handle_topic_event=None):
    message_type = message.get_message_type()
    if message_type == _EVENT_TOPIC_MESSAGE_TYPE and handle_topic_event is not None:
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        publish_time = FixSizedTypesCodec.decode_long(buf, _EVENT_TOPIC_PUBLISH_TIME_OFFSET)
        uuid = FixSizedTypesCodec.decode_uuid(buf, _EVENT_TOPIC_UUID_OFFSET)
        item = DataCodec.decode(message)
        handle_topic_event(item, publish_time, uuid)
        return
