from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.builtin import EntryListCodec

# hex: 0x013000
_REQUEST_MESSAGE_TYPE = 77824
# hex: 0x013001
_RESPONSE_MESSAGE_TYPE = 77825

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, entry_processor):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    StringCodec.encode(message, name)
    DataCodec.encode(message, entry_processor)
    return message


def decode_response(message):
    message.next_frame()
    return EntryListCodec.decode(message, DataCodec.decode, DataCodec.decode)
