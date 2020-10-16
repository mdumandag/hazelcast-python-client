from hazelcast.protocol.client_message import OutboundMessage, REQUEST_HEADER_SIZE, create_initial_buffer
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
    buf = create_initial_buffer(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    StringCodec.encode(buf, name)
    PagingPredicateHolderCodec.encode(buf, predicate, True)
    return OutboundMessage(buf, True)


def decode_response(msg):
    msg.next_frame()
    response = dict()
    response["response"] = ListMultiFrameCodec.decode(msg, DataCodec.decode)
    response["anchor_data_list"] = AnchorDataListHolderCodec.decode(msg)
    return response
