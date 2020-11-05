from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.distributed_object_info_codec import DistributedObjectInfoCodec

# hex: 0x000800
_REQUEST_MESSAGE_TYPE = 2048
# hex: 0x000801
_RESPONSE_MESSAGE_TYPE = 2049

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request():
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = False
    return message


def decode_response(message):
    message.next_frame()
    return ListMultiFrameCodec.decode(message, DistributedObjectInfoCodec.decode)
