from hazelcast.protocol.builtin import CodecUtil
from hazelcast.protocol.client_message import END_FRAME, BEGIN_FRAME
from hazelcast.protocol.builtin import ListIntegerCodec
from hazelcast.protocol import AnchorDataListHolder
from hazelcast.protocol.builtin import EntryListCodec
from hazelcast.protocol.builtin import DataCodec


class AnchorDataListHolderCodec(object):
    @staticmethod
    def encode(message, anchor_data_list_holder):
        message.add_frame(BEGIN_FRAME.copy())
        ListIntegerCodec.encode(message, anchor_data_list_holder.anchor_page_list)
        EntryListCodec.encode(message, anchor_data_list_holder.anchor_data_list, DataCodec.encode, DataCodec.encode)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def decode(message):
        message.next_frame()
        anchor_page_list = ListIntegerCodec.decode(message)
        anchor_data_list = EntryListCodec.decode(message, DataCodec.decode, DataCodec.decode)
        CodecUtil.fast_forward_to_end_frame(message)
        return AnchorDataListHolder(anchor_page_list, anchor_data_list)
