from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import EntryListCodec
from hazelcast.protocol.builtin import StringCodec

# hex: 0x000E00
_REQUEST_MESSAGE_TYPE = 3584
# hex: 0x000E01
_RESPONSE_MESSAGE_TYPE = 3585

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(proxies):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    EntryListCodec.encode(message, proxies, StringCodec.encode, StringCodec.encode)
    return message
