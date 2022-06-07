from enum import IntEnum


class ToMultiplexer(IntEnum):
    WRITE_SERIAL_PORT = 0  # u8 len; data[len]
    WRITE_MULTIPLEXED = 1  # u8 len; data[len]
    RESET_SERIAL_PORT = 2


class FromMultiplexer(IntEnum):
    FROM_SERIAL_PORT = 0  # u8 len; data[len]
    TO_SERIAL_PORT = 1  # u8 len; data[len]


