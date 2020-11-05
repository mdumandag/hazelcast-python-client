from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE, EVENT_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec

# hex: 0x000900
_REQUEST_MESSAGE_TYPE = 2304
# hex: 0x000901
_RESPONSE_MESSAGE_TYPE = 2305
# hex: 0x000902
_EVENT_DISTRIBUTED_OBJECT_MESSAGE_TYPE = 2306

_REQUEST_LOCAL_ONLY_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_LOCAL_ONLY_OFFSET + BOOLEAN_SIZE_IN_BYTES
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE
_EVENT_DISTRIBUTED_OBJECT_SOURCE_OFFSET = EVENT_HEADER_SIZE


def encode_request(local_only):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_boolean(buf, _REQUEST_LOCAL_ONLY_OFFSET, local_only)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    return FixSizedTypesCodec.decode_uuid(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)


def handle(message, handle_distributed_object_event=None):
    message_type = message.get_message_type()
    if message_type == _EVENT_DISTRIBUTED_OBJECT_MESSAGE_TYPE and handle_distributed_object_event is not None:
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        source = FixSizedTypesCodec.decode_uuid(buf, _EVENT_DISTRIBUTED_OBJECT_SOURCE_OFFSET)
        name = StringCodec.decode(message)
        service_name = StringCodec.decode(message)
        event_type = StringCodec.decode(message)
        handle_distributed_object_event(name, service_name, event_type, source)
        return
