from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec

# hex: 0x060900
_REQUEST_MESSAGE_TYPE = 395520
# hex: 0x060901
_RESPONSE_MESSAGE_TYPE = 395521

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    StringCodec.encode(message, name)
    return message
