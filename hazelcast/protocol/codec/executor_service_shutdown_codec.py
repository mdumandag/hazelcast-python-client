from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec

# hex: 0x080100
_REQUEST_MESSAGE_TYPE = 524544
# hex: 0x080101
_RESPONSE_MESSAGE_TYPE = 524545

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    StringCodec.encode(message, name)
    return message
