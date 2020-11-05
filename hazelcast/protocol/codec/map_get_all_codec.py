from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import EntryListCodec

# hex: 0x012300
_REQUEST_MESSAGE_TYPE = 74496
# hex: 0x012301
_RESPONSE_MESSAGE_TYPE = 74497

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, keys):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    StringCodec.encode(message, name)
    ListMultiFrameCodec.encode(message, keys, DataCodec.encode)
    return message


def decode_response(message):
    message.next_frame()
    return EntryListCodec.decode(message, DataCodec.decode, DataCodec.decode)
