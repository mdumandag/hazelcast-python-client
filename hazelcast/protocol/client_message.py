import errno
import socket

from hazelcast.serialization.bits import *

SIZE_OF_FRAME_LENGTH_AND_FLAGS = INT_SIZE_IN_BYTES + SHORT_SIZE_IN_BYTES

_MESSAGE_TYPE_OFFSET = 0
_CORRELATION_ID_OFFSET = _MESSAGE_TYPE_OFFSET + INT_SIZE_IN_BYTES
_RESPONSE_BACKUP_ACKS_OFFSET = _CORRELATION_ID_OFFSET + LONG_SIZE_IN_BYTES
_PARTITION_ID_OFFSET = _CORRELATION_ID_OFFSET + LONG_SIZE_IN_BYTES
_FRAGMENTATION_ID_OFFSET = 0

REQUEST_HEADER_SIZE = _PARTITION_ID_OFFSET + INT_SIZE_IN_BYTES
RESPONSE_HEADER_SIZE = _RESPONSE_BACKUP_ACKS_OFFSET + BYTE_SIZE_IN_BYTES
EVENT_HEADER_SIZE = _PARTITION_ID_OFFSET + INT_SIZE_IN_BYTES

_DEFAULT_FLAGS = 0
_BEGIN_FRAGMENT_FLAG = 1 << 15
_END_FRAGMENT_FLAG = 1 << 14
_UNFRAGMENTED_MESSAGE_FLAGS = _BEGIN_FRAGMENT_FLAG | _END_FRAGMENT_FLAG
_IS_FINAL_FLAG = 1 << 13
_BEGIN_DATA_STRUCTURE_FLAG = 1 << 12
_END_DATA_STRUCTURE_FLAG = 1 << 11
_IS_NULL_FLAG = 1 << 10
_IS_EVENT_FLAG = 1 << 9
_IS_BACKUP_AWARE_FLAG = 1 << 8
_IS_BACKUP_EVENT_FLAG = 1 << 7

_FRAME_HEADER_BUF = bytearray(SIZE_OF_FRAME_LENGTH_AND_FLAGS)


# For codecs
def create_initial_frame(size, message_type):
    buf = bytearray(size)
    LE_INT.pack_into(buf, _MESSAGE_TYPE_OFFSET, message_type)
    LE_INT.pack_into(buf, _PARTITION_ID_OFFSET, -1)
    frame = Frame(buf, _UNFRAGMENTED_MESSAGE_FLAGS)
    return frame


# For custom codecs
def create_initial_frame_custom(size, add_flag=False):
    buf = bytearray(size)
    flags = _DEFAULT_FLAGS
    if add_flag:
        flags |= _BEGIN_DATA_STRUCTURE_FLAG
    return Frame(buf, flags)


class ClientMessage(object):
    __slots__ = ("start_frame", "end_frame", "retryable", "_next_frame")

    def __init__(self, start_frame):
        self.start_frame = start_frame
        self.end_frame = start_frame
        self._next_frame = start_frame

    def next_frame(self):
        result = self._next_frame
        if self._next_frame is not None:
            self._next_frame = self._next_frame.next
        return result

    def has_next_frame(self):
        return self._next_frame is not None

    def peek_next_frame(self):
        return self._next_frame

    def add_frame(self, frame):
        frame.next = None
        self.end_frame.next = frame
        self.end_frame = frame

    def get_message_type(self):
        return LE_INT.unpack_from(self.start_frame.buf, _MESSAGE_TYPE_OFFSET)[0]

    def get_correlation_id(self):
        return LE_LONG.unpack_from(self.start_frame.buf, _CORRELATION_ID_OFFSET)[0]

    def set_correlation_id(self, correlation_id):
        LE_LONG.pack_into(self.start_frame.buf, _CORRELATION_ID_OFFSET, correlation_id)

    def set_partition_id(self, partition_id):
        LE_INT.pack_into(self.start_frame.buf, _PARTITION_ID_OFFSET, partition_id)

    def get_number_of_backup_acks(self):
        return LE_UINT8.unpack_from(self.start_frame.buf, _RESPONSE_BACKUP_ACKS_OFFSET)[0]

    def get_fragmentation_id(self):
        return LE_LONG.unpack_from(self.start_frame.buf, _FRAGMENTATION_ID_OFFSET)[0]

    def merge(self, fragment):
        # should be called after calling drop_fragmentation_frame() on fragment
        self.end_frame.next = fragment.start_frame
        self.end_frame = fragment.end_frame

    def drop_fragmentation_frame(self):
        self.start_frame = self.start_frame.next
        self._next_frame = self.start_frame

    def copy(self):
        message = ClientMessage(self.start_frame.deep_copy())
        message.end_frame = self.end_frame
        message.retryable = self.retryable
        return message

    def set_backup_aware_flag(self):
        self.start_frame.flags |= _IS_BACKUP_AWARE_FLAG

    def write_to(self, buf):
        cur = self.start_frame
        while cur:
            b = cur.buf
            flags = cur.flags
            if not cur.next:
                flags |= _IS_FINAL_FLAG
            LE_INT.pack_into(_FRAME_HEADER_BUF, 0, len(b) + SIZE_OF_FRAME_LENGTH_AND_FLAGS)
            LE_UINT16.pack_into(_FRAME_HEADER_BUF, 4, flags)
            buf.write(_FRAME_HEADER_BUF)
            buf.write(b)
            cur = cur.next

    def __repr__(self):
        message_type = self.get_message_type()
        correlation_id = self.get_correlation_id()
        return "OutboundMessage(message_type=%s, correlation_id=%s, retryable=%s, %s)" \
               % (message_type, correlation_id, self.retryable, self.start_frame.flags)


class Frame(object):
    __slots__ = ("buf", "flags", "next")

    def __init__(self, buf, flags=_DEFAULT_FLAGS):
        self.buf = buf
        self.flags = flags
        self.next = None

    def copy(self):
        frame = Frame(self.buf, self.flags)
        frame.next = self.next
        return frame

    def deep_copy(self):
        frame = Frame(self.buf.copy(), self.flags)
        frame.next = self.next
        return frame

    def is_begin_frame(self):
        return self._is_flag_set(_BEGIN_DATA_STRUCTURE_FLAG)

    def is_end_frame(self):
        return self._is_flag_set(_END_DATA_STRUCTURE_FLAG)

    def is_null_frame(self):
        return self._is_flag_set(_IS_NULL_FLAG)

    def is_final_frame(self):
        return self._is_flag_set(_IS_FINAL_FLAG)

    def has_event_flag(self):
        return self._is_flag_set(_IS_EVENT_FLAG)

    def has_backup_event_flag(self):
        return self._is_flag_set(_IS_BACKUP_EVENT_FLAG)

    def has_unfragmented_message_flags(self):
        return self._is_flag_set(_UNFRAGMENTED_MESSAGE_FLAGS)

    def has_begin_fragment_flag(self):
        return self._is_flag_set(_BEGIN_FRAGMENT_FLAG)

    def has_end_fragment_flag(self):
        return self._is_flag_set(_END_FRAGMENT_FLAG)

    def _is_flag_set(self, flag_mask):
        i = self.flags & flag_mask
        return i == flag_mask


NULL_FRAME = Frame(bytearray(0), _IS_NULL_FLAG)
BEGIN_FRAME = Frame(bytearray(0), _BEGIN_DATA_STRUCTURE_FLAG)
END_FRAME = Frame(bytearray(0), _END_DATA_STRUCTURE_FLAG)


class ClientMessageBuilder(object):
    def __init__(self, message_callback):
        self._fragmented_messages = dict()
        self._message_callback = message_callback

    def on_message(self, client_message):
        if client_message.start_frame.has_unfragmented_message_flags():
            self._message_callback(client_message)
        else:
            fragmentation_frame = client_message.start_frame
            fragmentation_id = client_message.get_fragmentation_id()
            client_message.drop_fragmentation_frame()
            if fragmentation_frame.has_begin_fragment_flag():
                self._fragmented_messages[fragmentation_id] = client_message
            else:
                existing_message = self._fragmented_messages.get(fragmentation_id, None)
                if not existing_message:
                    raise socket.error(errno.EIO, "A message without the begin part is received.")

                existing_message.merge(client_message)
                if fragmentation_frame.has_end_fragment_flag():
                    self._message_callback(existing_message)
                    del self._fragmented_messages[fragmentation_id]
