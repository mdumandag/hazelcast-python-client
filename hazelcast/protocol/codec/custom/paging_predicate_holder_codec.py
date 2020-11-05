from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME, create_initial_frame_custom, BEGIN_FRAME
from hazelcast.protocol import PagingPredicateHolder
from hazelcast.protocol.codec.custom.anchor_data_list_holder_codec import AnchorDataListHolderCodec
from hazelcast.protocol.builtin import DataCodec


_PAGE_SIZE_OFFSET = 0
_PAGE_OFFSET = _PAGE_SIZE_OFFSET + INT_SIZE_IN_BYTES
_ITERATION_TYPE_ID_OFFSET = _PAGE_OFFSET + INT_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _ITERATION_TYPE_ID_OFFSET + BYTE_SIZE_IN_BYTES


class PagingPredicateHolderCodec(object):
    @staticmethod
    def encode(message, paging_predicate_holder):
        message.add_frame(BEGIN_FRAME.copy())
        initial_frame = create_initial_frame_custom(_INITIAL_FRAME_SIZE)
        buf = initial_frame.buf
        FixSizedTypesCodec.encode_int(buf, _PAGE_SIZE_OFFSET, paging_predicate_holder.page_size)
        FixSizedTypesCodec.encode_int(buf, _PAGE_OFFSET, paging_predicate_holder.page)
        FixSizedTypesCodec.encode_byte(buf, _ITERATION_TYPE_ID_OFFSET, paging_predicate_holder.iteration_type_id)
        message.add_frame(initial_frame)
        AnchorDataListHolderCodec.encode(message, paging_predicate_holder.anchor_data_list_holder)
        CodecUtil.encode_nullable(message, paging_predicate_holder.predicate_data, DataCodec.encode)
        CodecUtil.encode_nullable(message, paging_predicate_holder.comparator_data, DataCodec.encode)
        CodecUtil.encode_nullable(message, paging_predicate_holder.partition_key_data, DataCodec.encode)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def decode(message):
        message.next_frame()
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        page_size = FixSizedTypesCodec.decode_int(buf, _PAGE_SIZE_OFFSET)
        page = FixSizedTypesCodec.decode_int(buf, _PAGE_OFFSET)
        iteration_type_id = FixSizedTypesCodec.decode_byte(buf, _ITERATION_TYPE_ID_OFFSET)
        anchor_data_list_holder = AnchorDataListHolderCodec.decode(message)
        predicate_data = CodecUtil.decode_nullable(message, DataCodec.decode)
        comparator_data = CodecUtil.decode_nullable(message, DataCodec.decode)
        partition_key_data = CodecUtil.decode_nullable(message, DataCodec.decode)
        CodecUtil.fast_forward_to_end_frame(message)
        return PagingPredicateHolder(anchor_data_list_holder, predicate_data, comparator_data, page_size, page, iteration_type_id, partition_key_data)
