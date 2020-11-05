from hazelcast.serialization.bits import *
from hazelcast.protocol.builtin import FixSizedTypesCodec
from hazelcast.protocol.client_message import ClientMessage, REQUEST_HEADER_SIZE, create_initial_frame, RESPONSE_HEADER_SIZE
from hazelcast.protocol.builtin import StringCodec
from hazelcast.protocol.builtin import CodecUtil
from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.address_codec import AddressCodec

# hex: 0x000100
_REQUEST_MESSAGE_TYPE = 256
# hex: 0x000101
_RESPONSE_MESSAGE_TYPE = 257

_REQUEST_UUID_OFFSET = REQUEST_HEADER_SIZE
_REQUEST_SERIALIZATION_VERSION_OFFSET = _REQUEST_UUID_OFFSET + UUID_SIZE_IN_BYTES
_REQUEST_INITIAL_FRAME_SIZE = _REQUEST_SERIALIZATION_VERSION_OFFSET + BYTE_SIZE_IN_BYTES
_RESPONSE_STATUS_OFFSET = RESPONSE_HEADER_SIZE
_RESPONSE_MEMBER_UUID_OFFSET = _RESPONSE_STATUS_OFFSET + BYTE_SIZE_IN_BYTES
_RESPONSE_SERIALIZATION_VERSION_OFFSET = _RESPONSE_MEMBER_UUID_OFFSET + UUID_SIZE_IN_BYTES
_RESPONSE_PARTITION_COUNT_OFFSET = _RESPONSE_SERIALIZATION_VERSION_OFFSET + BYTE_SIZE_IN_BYTES
_RESPONSE_CLUSTER_ID_OFFSET = _RESPONSE_PARTITION_COUNT_OFFSET + INT_SIZE_IN_BYTES
_RESPONSE_FAILOVER_SUPPORTED_OFFSET = _RESPONSE_CLUSTER_ID_OFFSET + UUID_SIZE_IN_BYTES


def encode_request(cluster_name, username, password, uuid, client_type, serialization_version, client_hazelcast_version, client_name, labels):
    initial_frame = create_initial_frame(_REQUEST_INITIAL_FRAME_SIZE, _REQUEST_MESSAGE_TYPE)
    message = ClientMessage(initial_frame)
    message.retryable = True
    buf = initial_frame.buf
    FixSizedTypesCodec.encode_uuid(buf, _REQUEST_UUID_OFFSET, uuid)
    FixSizedTypesCodec.encode_byte(buf, _REQUEST_SERIALIZATION_VERSION_OFFSET, serialization_version)
    StringCodec.encode(message, cluster_name)
    CodecUtil.encode_nullable(message, username, StringCodec.encode)
    CodecUtil.encode_nullable(message, password, StringCodec.encode)
    StringCodec.encode(message, client_type)
    StringCodec.encode(message, client_hazelcast_version)
    StringCodec.encode(message, client_name)
    ListMultiFrameCodec.encode(message, labels, StringCodec.encode)
    return message


def decode_response(message):
    initial_frame = message.next_frame()
    response = dict()
    buf = initial_frame.buf
    response["status"] = FixSizedTypesCodec.decode_byte(buf, _RESPONSE_STATUS_OFFSET)
    response["member_uuid"] = FixSizedTypesCodec.decode_uuid(buf, _RESPONSE_MEMBER_UUID_OFFSET)
    response["serialization_version"] = FixSizedTypesCodec.decode_byte(buf, _RESPONSE_SERIALIZATION_VERSION_OFFSET)
    response["partition_count"] = FixSizedTypesCodec.decode_int(buf, _RESPONSE_PARTITION_COUNT_OFFSET)
    response["cluster_id"] = FixSizedTypesCodec.decode_uuid(buf, _RESPONSE_CLUSTER_ID_OFFSET)
    response["failover_supported"] = FixSizedTypesCodec.decode_boolean(buf, _RESPONSE_FAILOVER_SUPPORTED_OFFSET)
    response["address"] = CodecUtil.decode_nullable(message, AddressCodec.decode)
    response["server_hazelcast_version"] = StringCodec.decode(message)
    return response
