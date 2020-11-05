from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.codec.custom.paging_predicate_holder_codec import PagingPredicateHolderCodec
from hazelcast.protocol.builtin import EntryListCodec
from hazelcast.protocol.builtin import DataCodec
from hazelcast.protocol.codec.custom.anchor_data_list_holder_codec import AnchorDataListHolderCodec

# hex: 0x013600
_REQUEST_MESSAGE_TYPE = 79360
# hex: 0x013601
_RESPONSE_MESSAGE_TYPE = 79361

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
    response["response"] = EntryListCodec.decode(message, DataCodec.decode, DataCodec.decode)
    response["anchor_data_list"] = AnchorDataListHolderCodec.decode(message)
    return response
