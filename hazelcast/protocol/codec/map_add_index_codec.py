from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.codec.custom.index_config_codec import IndexConfigCodec

# hex: 0x012900
_REQUEST_MESSAGE_TYPE = 76032
# hex: 0x012901
_RESPONSE_MESSAGE_TYPE = 76033

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, index_config):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    StringCodec.encode(message, name)
    IndexConfigCodec.encode(message, index_config)
    return message
