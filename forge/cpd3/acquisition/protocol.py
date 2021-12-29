from enum import IntEnum, auto


class ToServerPacketType(IntEnum):
    HELLO = 0  # No data
    PING = auto()  # No data
    REALTIME_RESEND = auto()  # No data
    START_ARCHIVE_READ = auto()  # uint32_t size; uint8_t data[size];
    ABORT_ARCHIVE_READ = auto()  # No data
    MESSAGE_LOG_EVENT = auto()  # uint32_t size; uint8_t data[size];
    COMMAND = auto()  # uint16_t name_length; char name[name_length]; uint32_t data_length; uint8_t data[data_length];
    SYSTEM_FLUSH = auto()  # double duration;
    SET_AVERAGING_TIME = auto()  # uint8_t unit; int32_t count; uint8_t align;
    DATA_FLUSH = auto()  # No data
    BYPASS_FLAG_SET = auto()  # uint16_t size; char flag[size];
    BYPASS_FLAG_CLEAR = auto()  # uint16_t size; char flag[size];
    BYPASS_FLAGS_CLEAR_ALL = auto()  # No data
    SYSTEM_FLAG_SET = auto()  # uint16_t size; char flag[size];
    SYSTEM_FLAG_CLEAR = auto()  # uint16_t size; char flag[size];
    SYSTEM_FLAGS_CLEAR_ALL = auto()  # No data
    RESTART_REQUEST = auto()


class FromServerPacketType(IntEnum):
    HELLO = 0  # No data
    PONG = auto()  # No data
    EVENT = auto()  # uint32_t size;  uint8_t data[size];
    AUTOPROBE_STATE = auto()  # uint32_t size; uint8_t data[size];
    INTERFACE_INFORMATION = auto()  # uint16_t name_length; char name[name_length]; uint32_t data_length; uint8_t data[data_length];
    INTERFACE_STATE = auto()  # uint16_t name_length; char name[name_length]; uint32_t data_length; uint8_t data[data_length];
    REALTIME_NAME = auto()  # uint32_t size; uint8_t data[size];
    REALTIME_VALUE = auto()  # uint16_t name; uint32_t size; uint8_t data[size];
    ARCHIVE_VALUE = auto()  # uint32_t size; uint8_t data[size];
    ARCHIVE_END = auto()  # No data
