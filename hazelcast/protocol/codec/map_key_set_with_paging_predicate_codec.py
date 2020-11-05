from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.codec.custom.paging_predicate_holder_codec import PagingPredicateHolderCodec
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.codec.custom.anchor_data_list_holder_codec import AnchorDataListHolderCodec

# hex: 0x013400
_REQUEST_MESSAGE_TYPE = 78848
# hex: 0x013401
_RESPONSE_MESSAGE_TYPE = 78849

_REQUEST_INITIAL_FRAME_SIZE = REQUEST_HEADER_SIZE


def encode_request(name, predicate):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    StringCodec.encode(message, name)
    PagingPredicateHolderCodec.encode(message, predicate)
    return message


def decode_response(message):
    message.next_frame()
    response = dict()
    response["response"] = ListMultiFrameCodec.decode(message, DataCodec.decode)
    response["anchor_data_list"] = AnchorDataListHolderCodec.decode(message)
    return response
