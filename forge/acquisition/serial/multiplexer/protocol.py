from enum import IntEnum


class ToMultiplexer(IntEnum):
    WRITE_SERIAL_PORT = 0  # u8 len; data[len]
    WRITE_MULTIPLEXED = 1  # u8 len; data[len]
    RESET_SERIAL_PORT = 2


class FromMultiplexer(IntEnum):
    FROM_SERIAL_PORT = 0  # u8 len; data[len]
    TO_SERIAL_PORT = 1  # u8 len; data[len]


class Parity(IntEnum):
    NONE = 0
    EVEN = 1
    ODD = 2
    MARK = 3
    SPACE = 4


class ControlOperation(IntEnum):
    SET_BAUD = 0  # u32 baud
    SET_DATA_BITS = 1  # u8 bits
    SET_PARITY = 2  # Parity(u8)
    SET_STOP_BITS = 3  # u8 bits
    SET_RS485 = 4  # u8 rts_for_tx; u8 rts_for_rx; u8 loopback; f32 before_tx; f32 before_rx
    SET_RTS = 5  # u8 rts
    SET_DTR = 6  # u8 dtr
    FLUSH = 7
    BREAK = 8
    REOPEN = 9


