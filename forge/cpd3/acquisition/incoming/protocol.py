from enum import IntEnum


PROTOCOL_VERSION = 1


class PacketFromAcquisition(IntEnum):
    DATA_BLOCK_BEGIN = 0
    DEFINE_NAMES = 1
    EVENT = 2
    AUTOPROBE_STATE = 3
    INTERFACE_INFORMATION = 4
    INTERFACE_STATE = 5


class PacketToAcquisition(IntEnum):
    MESSAGE_LOG = 0
    COMMAND = 1
    BYPASS_FLAG_SET = 2
    BYPASS_FLAG_CLEAR = 3
    BYPASS_FLAGS_CLEAR_ALL = 4
    SYSTEM_FLAG_SET = 5
    SYSTEM_FLAG_CLEAR = 6
    SYSTEM_FLAGS_CLEAR_ALL = 7
    SYSTEM_FLUSH = 8
    RESTART_ACQUISITION_SYSTEM = 9


class DataBlockType(IntEnum):
    FLOATS = 0
    ARRAYS_OF_FLOATS = 1
    VARIANT = 2
    FINAL = 0xFF
