import uuid

from hazelcast import six
from hazelcast.six.moves import range
from hazelcast.protocol.client_message import NULL_FRAME, BEGIN_FRAME, END_FRAME, \
    SIZE_OF_FRAME_LENGTH_AND_FLAGS, _IS_FINAL_FLAG, Frame
from hazelcast.serialization import LONG_SIZE_IN_BYTES, UUID_SIZE_IN_BYTES, LE_INT, LE_LONG, BOOLEAN_SIZE_IN_BYTES, \
    INT_SIZE_IN_BYTES, LE_ULONG, LE_UINT16, LE_INT8, UUID_MSB_SHIFT, UUID_LSB_MASK
from hazelcast.serialization.data import Data


class CodecUtil(object):
    @staticmethod
    def fast_forward_to_end_frame(message):
        # We are starting from 1 because of the BEGIN_FRAME we read
        # in the beginning of the decode method
        num_expected_end_frames = 1
        while num_expected_end_frames != 0:
            frame = message.next_frame()
            if frame.is_end_frame():
                num_expected_end_frames -= 1
            elif frame.is_begin_frame():
                num_expected_end_frames += 1

    @staticmethod
    def encode_nullable(message, value, encoder):
        if value is None:
            message.add_frame(NULL_FRAME.copy())
        else:
            encoder(message, value)

    @staticmethod
    def decode_nullable(message, decoder):
        if CodecUtil.next_frame_is_null_frame(message):
            return None
        else:
            return decoder(message)

    @staticmethod
    def next_frame_is_data_structure_end_frame(message):
        return message.peek_next_frame().is_end_frame()

    @staticmethod
    def next_frame_is_null_frame(message):
        is_null = message.peek_next_frame().is_null_frame()
        if is_null:
            message.next_frame()
        return is_null


class ByteArrayCodec(object):
    @staticmethod
    def encode(message, value):
        message.add_frame(Frame(value))

    @staticmethod
    def decode(message):
        return message.next_frame().buf


class DataCodec(object):
    @staticmethod
    def encode(message, value):
        message.add_frame(Frame(value.to_bytes()))

    @staticmethod
    def decode(message):
        return Data(message.next_frame().buf)

    @staticmethod
    def encode_nullable(message, value):
        if value is None:
            message.add_frame(NULL_FRAME.copy())
        else:
            DataCodec.encode(message, value)

    @staticmethod
    def decode_nullable(message):
        if CodecUtil.next_frame_is_null_frame(message):
            return None
        else:
            return DataCodec.decode(message)


class EntryListCodec(object):
    @staticmethod
    def encode(message, entries, key_encoder, value_encoder):
        message.add_frame(BEGIN_FRAME.copy())
        for key, value in entries:
            key_encoder(message, key)
            value_encoder(message, value)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def encode_nullable(message, entries, key_encoder, value_encoder):
        if entries is None:
            message.add_frame(NULL_FRAME.copy())
        else:
            EntryListCodec.encode(message, entries, key_encoder, value_encoder)

    @staticmethod
    def decode(message, key_decoder, value_decoder):
        result = []
        message.next_frame()
        while not CodecUtil.next_frame_is_data_structure_end_frame(message):
            key = key_decoder(message)
            value = value_decoder(message)
            result.append((key, value))

        message.next_frame()
        return result

    @staticmethod
    def decode_nullable(message, key_decoder, value_decoder):
        if CodecUtil.next_frame_is_null_frame(message):
            return None
        else:
            return EntryListCodec.decode(message, key_decoder, value_decoder)


_UUID_LONG_ENTRY_SIZE_IN_BYTES = UUID_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES


class EntryListUUIDLongCodec(object):

    @staticmethod
    def encode(message, entries):
        n = len(entries)
        b = bytearray(n * _UUID_LONG_ENTRY_SIZE_IN_BYTES)
        for i in range(n):
            key, value = entries[i]
            o = i * _UUID_LONG_ENTRY_SIZE_IN_BYTES
            FixSizedTypesCodec.encode_uuid(b, o, key)
            FixSizedTypesCodec.encode_long(b, o + UUID_SIZE_IN_BYTES, value)
        message.add_frame(Frame(b))

    @staticmethod
    def decode(message):
        b = message.next_frame().buf
        n = len(b) // _UUID_LONG_ENTRY_SIZE_IN_BYTES
        result = []
        for i in range(n):
            o = i * _UUID_LONG_ENTRY_SIZE_IN_BYTES
            key = FixSizedTypesCodec.decode_uuid(b, o)
            value = FixSizedTypesCodec.decode_long(b, o + UUID_SIZE_IN_BYTES)
            result.append((key, value))
        return result


class EntryListUUIDListIntegerCodec(object):
    @staticmethod
    def encode(message, entries):
        keys = []
        message.add_frame(BEGIN_FRAME.copy())
        for key, value in entries:
            keys.append(key)
            ListIntegerCodec.encode(message, value)
        message.add_frame(END_FRAME.copy())
        ListUUIDCodec.encode(message, keys)

    @staticmethod
    def decode(message):
        values = ListMultiFrameCodec.decode(message, ListIntegerCodec.decode)
        keys = ListUUIDCodec.decode(message)
        result = []
        n = len(keys)
        for i in range(n):
            result.append((keys[i], values[i]))
        return result


class FixSizedTypesCodec(object):
    @staticmethod
    def encode_int(buf, offset, value):
        LE_INT.pack_into(buf, offset, value)

    @staticmethod
    def decode_int(buf, offset):
        return LE_INT.unpack_from(buf, offset)[0]

    @staticmethod
    def encode_long(buf, offset, value):
        LE_LONG.pack_into(buf, offset, value)

    @staticmethod
    def decode_long(buf, offset):
        return LE_LONG.unpack_from(buf, offset)[0]

    @staticmethod
    def encode_boolean(buf, offset, value):
        if value:
            LE_INT8.pack_into(buf, offset, 1)
        else:
            LE_INT8.pack_into(buf, offset, 0)

    @staticmethod
    def decode_boolean(buf, offset):
        return LE_INT8.unpack_from(buf, offset)[0] == 1

    @staticmethod
    def encode_byte(buf, offset, value):
        LE_INT8.pack_into(buf, offset, value)

    @staticmethod
    def decode_byte(buf, offset):
        return LE_INT8.unpack_from(buf, offset)[0]

    @staticmethod
    def encode_uuid(buf, offset, value):
        is_null = value is None
        FixSizedTypesCodec.encode_boolean(buf, offset, is_null)
        if is_null:
            return

        o = offset + BOOLEAN_SIZE_IN_BYTES
        LE_ULONG.pack_into(buf, o, value.int >> UUID_MSB_SHIFT)
        LE_ULONG.pack_into(buf, o + LONG_SIZE_IN_BYTES, value.int & UUID_LSB_MASK)

    @staticmethod
    def decode_uuid(buf, offset):
        is_null = FixSizedTypesCodec.decode_boolean(buf, offset)
        if is_null:
            return None

        msb_offset = offset + BOOLEAN_SIZE_IN_BYTES
        lsb_offset = msb_offset + LONG_SIZE_IN_BYTES
        b = buf[lsb_offset - 1:msb_offset - 1:-1] + buf[lsb_offset + LONG_SIZE_IN_BYTES - 1:lsb_offset - 1:-1]
        return uuid.UUID(bytes=bytes(b))


class ListIntegerCodec(object):
    @staticmethod
    def encode(message, arr):
        n = len(arr)
        b = bytearray(n * INT_SIZE_IN_BYTES)
        for i in range(n):
            FixSizedTypesCodec.encode_int(b, i * INT_SIZE_IN_BYTES, arr[i])
        message.add_frame(Frame(b))

    @staticmethod
    def decode(message):
        b = message.next_frame().buf
        n = len(b) // INT_SIZE_IN_BYTES
        result = []
        for i in range(n):
            result.append(FixSizedTypesCodec.decode_int(b, i * INT_SIZE_IN_BYTES))
        return result


class ListLongCodec(object):
    @staticmethod
    def encode(message, arr):
        n = len(arr)
        b = bytearray(n * LONG_SIZE_IN_BYTES)
        for i in range(n):
            FixSizedTypesCodec.encode_long(b, i * LONG_SIZE_IN_BYTES, arr[i])
        message.add_frame(Frame(b))

    @staticmethod
    def decode(message):
        b = message.next_frame().buf
        n = len(b) // LONG_SIZE_IN_BYTES
        result = []
        for i in range(n):
            result.append(FixSizedTypesCodec.decode_long(b, i * LONG_SIZE_IN_BYTES))
        return result


class ListMultiFrameCodec(object):
    @staticmethod
    def encode(message, arr, encoder):
        message.add_frame(BEGIN_FRAME.copy())
        for item in arr:
            encoder(message, item)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def encode_contains_nullable(message, arr, encoder):
        message.add_frame(BEGIN_FRAME.copy())
        for item in arr:
            if item is None:
                message.add_frame(NULL_FRAME.copy())
            else:
                encoder(message, item)
        message.add_frame(END_FRAME.copy())

    @staticmethod
    def encode_nullable(message, arr, encoder):
        if arr is None:
            message.add_frame(NULL_FRAME.copy())
        else:
            ListMultiFrameCodec.encode(message, arr, encoder)

    @staticmethod
    def decode(message, decoder):
        result = []
        message.next_frame()
        while not CodecUtil.next_frame_is_data_structure_end_frame(message):
            result.append(decoder(message))

        message.next_frame()
        return result

    @staticmethod
    def decode_contains_nullable(message, decoder):
        result = []
        message.next_frame()
        while not CodecUtil.next_frame_is_data_structure_end_frame(message):
            if CodecUtil.next_frame_is_null_frame(message):
                result.append(None)
            else:
                result.append(decoder(message))

        message.next_frame()
        return result

    @staticmethod
    def decode_nullable(message, decoder):
        if CodecUtil.next_frame_is_null_frame(message):
            return None
        else:
            return ListMultiFrameCodec.decode(message, decoder)


class ListUUIDCodec(object):
    @staticmethod
    def encode(message, arr):
        n = len(arr)
        b = bytearray(n * UUID_SIZE_IN_BYTES)
        for i in range(n):
            FixSizedTypesCodec.encode_uuid(b, i * UUID_SIZE_IN_BYTES, arr[i])
        message.add_frame(Frame(b))

    @staticmethod
    def decode(message):
        b = message.next_frame().buf
        n = len(b) // UUID_SIZE_IN_BYTES
        result = []
        for i in range(n):
            result.append(FixSizedTypesCodec.decode_uuid(b, i * UUID_SIZE_IN_BYTES))
        return result


class LongArrayCodec(object):
    @staticmethod
    def encode(message, arr):
        n = len(arr)
        b = bytearray(n * LONG_SIZE_IN_BYTES)
        for i in range(n):
            FixSizedTypesCodec.encode_long(b, i * LONG_SIZE_IN_BYTES, arr[i])
        message.add_frame(Frame(b))

    @staticmethod
    def decode(message):
        b = message.next_frame().buf
        n = len(b) // LONG_SIZE_IN_BYTES
        result = []
        for i in range(n):
            result.append(FixSizedTypesCodec.decode_long(b, i * LONG_SIZE_IN_BYTES))
        return result


class MapCodec(object):
    @staticmethod
    def encode(message, m, key_encoder, value_encoder):
        message.add_frme(BEGIN_FRAME.copy())
        for key, value in six.iteritems(m):
            key_encoder(message, key)
            value_encoder(message, value)
        message.add_frme(END_FRAME.copy())

    @staticmethod
    def encode_nullable(message, m, key_encoder, value_encoder):
        if m is None:
            message.add_frame(NULL_FRAME.copy())
        else:
            MapCodec.encode(message, m, key_encoder, value_encoder)

    @staticmethod
    def decode(message, key_decoder, value_decoder):
        result = dict()
        message.next_frame()
        while not CodecUtil.next_frame_is_data_structure_end_frame(message):
            key = key_decoder(message)
            value = value_decoder(message)
            result[key] = value

        message.next_frame()
        return result

    @staticmethod
    def decode_nullable(message, key_decoder, value_decoder):
        if CodecUtil.next_frame_is_null_frame(message):
            return None
        else:
            return MapCodec.decode(message, key_decoder, value_decoder)


class StringCodec(object):
    @staticmethod
    def encode(message, value):
        value_bytes = value.encode("utf-8")
        message.add_frame(Frame(value_bytes))

    @staticmethod
    def decode(message):
        return message.next_frame().buf.decode("utf-8")
