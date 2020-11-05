from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x060A00
_REQUEST_MESSAGE_TYPE = 395776
# hex: 0x060A01
_RESPONSE_MESSAGE_TYPE = 395777

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    StringCodec.encode(message, name)
    return message


def decode_response(message):
    message.next_frame()
    return ListMultiFrameCodec.decode(message, DataCodec.decode)
