from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE, EVENT_HEADER_SIZE

# hex: 0x000F00
_REQUEST_MESSAGE_TYPE = 3840
# hex: 0x000F01
_RESPONSE_MESSAGE_TYPE = 3841
# hex: 0x000F02
_EVENT_BACKUP_MESSAGE_TYPE = 3842

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE
_RESPONSE_RESPONSE_OFFSET = RESPONSE_HEADER_SIZE
_EVENT_BACKUP_SOURCE_INVOCATION_CORRELATION_ID_OFFSET = EVENT_HEADER_SIZE


def encode_request():
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    return FixSizedTypesCodec.decode_uuid(initial_frame.buf, _RESPONSE_RESPONSE_OFFSET)


def handle(message, handle_backup_event=None):
    message_type = message.get_message_type()
    if message_type == _EVENT_BACKUP_MESSAGE_TYPE and handle_backup_event is not None:
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        source_invocation_correlation_id = FixSizedTypesCodec.decode_long(buf, _EVENT_BACKUP_SOURCE_INVOCATION_CORRELATION_ID_OFFSET)
        handle_backup_event(source_invocation_correlation_id)
        return
