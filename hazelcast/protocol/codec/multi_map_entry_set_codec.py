from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import EntryListCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x020600
_REQUEST_MESSAGE_TYPE = 132608
# hex: 0x020601
_RESPONSE_MESSAGE_TYPE = 132609

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    StringCodec.encode(message, name)
    return message


def decode_response(message):
    message.next_frame()
    return EntryListCodec.decode(message, DataCodec.decode, DataCodec.decode)
