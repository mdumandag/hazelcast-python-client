from hazelcast.protocol.builtin import FixSizedTypesCodec, CodecUtil
from hazelcast.serialization.bits import *
from hazelcast.protocol.client_message import END_FRAME, create_initial_frame_custom, BEGIN_FRAME
from hazelcast.core import MemberInfo
from hazelcast.protocol.codec.custom.address_codec import AddressCodec
from hazelcast.protocol.builtin import MapCodec
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.codec.custom.member_version_codec import MemberVersionCodec
from hazelcast.protocol.codec.custom.endpoint_qualifier_codec import EndpointQualifierCodec


_UUID_OFFSET = 0
_LITE_MEMBER_OFFSET = _UUID_OFFSET + UUID_SIZE_IN_BYTES
_INITIAL_FRAME_SIZE = _LITE_MEMBER_OFFSET + BOOLEAN_SIZE_IN_BYTES


class MemberInfoCodec(object):
    @staticmethod
    def encode(message, member_info):
        message.add_frame(BEGIN_FRAME.copy())
        initial_frame = create_initial_frame_custom(_INITIAL_FRAME_SIZE)
        buf = initial_frame.buf
        FixSizedTypesCodec.encode_uuid(buf, _UUID_OFFSET, member_info.uuid)
        FixSizedTypesCodec.encode_boolean(buf, _LITE_MEMBER_OFFSET, member_info.lite_member)
        message.add_frame(initial_frame)
        AddressCodec.encode(message, member_info.address)
        MapCodec.encode(message, member_info.attributes, StringCodec.encode, StringCodec.encode)
        MemberVersionCodec.encode(message, member_info.version)
        MapCodec.encode(message, member_info.address_map, EndpointQualifierCodec.encode, AddressCodec.encode)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def decode(message):
        message.next_frame()
        initial_frame = message.next_frame()
        buf = initial_frame.buf
        uuid = FixSizedTypesCodec.decode_uuid(buf, _UUID_OFFSET)
        lite_member = FixSizedTypesCodec.decode_boolean(buf, _LITE_MEMBER_OFFSET)
        address = AddressCodec.decode(message)
        attributes = MapCodec.decode(message, StringCodec.decode, StringCodec.decode)
        version = MemberVersionCodec.decode(message)
        is_address_map_exists = False
        address_map = None
        if not message.peek_next_frame().is_end_frame():
            address_map = MapCodec.decode(message, EndpointQualifierCodec.decode, AddressCodec.decode)
            is_address_map_exists = True
        CodecUtil.fast_forward_to_end_frame(message)
        return MemberInfo(address, uuid, attributes, lite_member, version, is_address_map_exists, address_map)
