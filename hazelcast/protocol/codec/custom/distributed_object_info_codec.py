from hazelcast.protocol.builtin import CodecUtil
from hazelcast.protocol.client_message import END_FRAME, BEGIN_FRAME
from hazelcast.protocol.builtin import StringCodec
from hazelcast.core import DistributedObjectInfo


class DistributedObjectInfoCodec(object):
    @staticmethod
    def encode(message, distributed_object_info):
        message.add_frame(BEGIN_FRAME.copy())
        StringCodec.encode(message, distributed_object_info.service_name)
        StringCodec.encode(message, distributed_object_info.name)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def decode(message):
        message.next_frame()
        service_name = StringCodec.decode(message)
        name = StringCodec.decode(message)
        CodecUtil.fast_forward_to_end_frame(message)
        return DistributedObjectInfo(service_name, name)
