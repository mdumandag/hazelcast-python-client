from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import EntryListCodec
from hazelcast.protocol.builtin import DataCodec

# hex: 0x0D0800
_REQUEST_MESSAGE_TYPE = 854016
# hex: 0x0D0801
_RESPONSE_MESSAGE_TYPE = 854017

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, entries):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    StringCodec.encode(message, name)
    EntryListCodec.encode(message, entries, DataCodec.encode, DataCodec.encode)
    return message
