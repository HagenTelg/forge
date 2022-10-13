import termios
from enum import IntEnum


class TCAttr(IntEnum):
    c_iflag = 0
    c_oflag = 1
    c_cflag = 2
    c_lflag = 3
    c_ispeed = 4
    c_ospeed = 5
    c_cc = 6


def standard_termios(tio) -> None:
    # Boilerplate for raw (non-escaped) with no echo or other buffering.
    tio[TCAttr.c_iflag.value] &= ~(termios.BRKINT | termios.PARMRK | termios.ISTRIP | termios.INLCR |
                                   termios.IGNCR | termios.ICRNL)
    tio[TCAttr.c_iflag.value] |= termios.IGNBRK
    tio[TCAttr.c_lflag.value] &= ~(termios.ICANON | termios.ECHO | termios.ECHOE | termios.ISIG | termios.IEXTEN)
    tio[TCAttr.c_oflag.value] &= ~termios.OPOST
    tio[TCAttr.c_cflag.value] |= termios.CREAD
    tio[TCAttr.c_cc.value][termios.VMIN] = 0
    tio[TCAttr.c_cc.value][termios.VTIME] = 0
