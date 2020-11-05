from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec

# hex: 0x012600
_REQUEST_MESSAGE_TYPE = 75264
# hex: 0x012601
_RESPONSE_MESSAGE_TYPE = 75265

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, predicate):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    StringCodec.encode(message, name)
    DataCodec.encode(message, predicate)
    return message


def decode_response(message):
    message.next_frame()
    return ListMultiFrameCodec.decode(message, DataCodec.decode)
