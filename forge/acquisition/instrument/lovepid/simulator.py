import typing
import asyncio
import logging
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.streaming import StreamingSimulator

_LOGGER = logging.getLogger(__name__)


class Simulator(StreamingSimulator):
    _ADDRESS_LETTER_CODES = {
        b'L': 0,
        b'O': 0x100,
        b'V': 0x200,
        b'E': 0x300,
    }

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.addresses: typing.List[int] = [0x31, 0x32, 0x33, 0x34]
        self.value: typing.List[float] = [float(i)+0.5 for i in range(len(self.addresses))]
        self.setpoint: typing.List[float] = [float(i)+0.75 for i in range(len(self.addresses))]
        self.output: typing.List[float] = [float(i)*10.0+5.0 for i in range(len(self.addresses))]
        self.manual_mode: typing.List[bool] = [False] * len(self.addresses)
        self.decimals: typing.List[int] = [2] * len(self.addresses)

        self.response_prefix: bytes = b""

    @staticmethod
    def _checksum(data: typing.Iterable[int]) -> bytes:
        s = 0
        for a in data:
            s += a
        return b'%02X' % (s & 0xFF)

    @staticmethod
    def _letter_for_address(address: int) -> bytes:
        address &= 0xF00
        for letter, check in Simulator._ADDRESS_LETTER_CODES.items():
            if check == address:
                return letter
        raise ValueError

    def _send_packet(self, address: int, payload: bytes) -> None:
        packet = bytearray([0x02])
        packet += self._letter_for_address(address)
        packet += b'%02X' % (address & 0xFF)
        packet += payload
        packet += self._checksum(packet[1:])
        packet.append(0x06)
        self.writer.write(self.response_prefix)
        self.writer.write(bytes(packet))

    def _send_error(self, address: int, error_code: int) -> None:
        packet = bytearray([0x02])
        packet += self._letter_for_address(address)
        packet += b'%02X' % (address & 0xFF)
        packet += b'N'
        packet += b'%02X' % error_code
        packet.append(0x06)

    def _send_number(self, address: int, value: float, decimals: int) -> None:
        if value < 0:
            bits = 0x01
            value = -value
        else:
            bits = 0
        bits |= decimals << 4
        value = round(value * (10 ** decimals))
        self._send_packet(address, b'%02X%04d' % (bits, value))

    async def run(self) -> typing.NoReturn:
        while True:
            try:
                await self.reader.readuntil(b'\x02')
            except (asyncio.IncompleteReadError, asyncio.LimitOverrunError):
                continue
            try:
                frame: bytes = await wait_cancelable(self.reader.readuntil(b'\x03'), 4.0)
            except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError):
                continue
            frame = frame[:-1]
            if len(frame) < 5:
                _LOGGER.debug(f"Frame {frame} too short")
                continue

            try:
                controller_identifier = frame[:1]
                controller_address = int(frame[1:3], 16)
                controller_address |= self._ADDRESS_LETTER_CODES[controller_identifier]
            except (TypeError, ValueError, KeyError):
                _LOGGER.debug(f"Invalid frame structure in {frame}")
                continue

            calculated_checksum = self._checksum(frame[1:-2])
            received_checksum = frame[-2:]
            if calculated_checksum != received_checksum.upper():
                _LOGGER.debug(f"Checksum mismatch in {frame}, got {received_checksum} but expected {calculated_checksum}")
                continue

            try:
                controller_index = self.addresses.index(controller_address)
            except ValueError:
                _LOGGER.debug(f"Controller {controller_address} not found")
                continue
            payload = frame[3:-2]

            try:
                if payload == b'00':
                    value = self.value[controller_index]
                    if value < 0:
                        sign = 1
                        value = -value
                    else:
                        sign = 0
                    value = round(value * (10 ** self.decimals[controller_index]))
                    self._send_packet(controller_address, (b'%X0%X%X%04d' % (
                        0xC if self.manual_mode[controller_index] else 0x4,
                        self.decimals[controller_index],
                        sign,
                        value,
                    )))
                elif payload == b'05':
                    self._send_packet(controller_address, b'0000000000')
                elif payload == b'0101' or payload == b'0153':
                    self._send_number(controller_address, self.setpoint[controller_index],
                                      self.decimals[controller_index])
                elif payload.startswith(b'0200'):
                    payload = payload[4:]
                    if len(payload) != 6:
                        raise ValueError
                    value = int(payload[:4]) / (10 ** self.decimals[controller_index])
                    sign = payload[4:]
                    if sign != b'00':
                        value = -value
                    self.setpoint[controller_index] = value
                    self._send_packet(controller_address, b'00')
                elif payload.startswith(b'0266'):
                    payload = payload[4:]
                    if len(payload) != 6:
                        raise ValueError
                    value = int(payload[:4]) / 10.0
                    sign = payload[4:]
                    if sign != b'00':
                        raise ValueError
                    self.setpoint[controller_index] = value
                    self._send_packet(controller_address, b'00')
                elif payload == b'0156':
                    self._send_number(controller_address, self.output[controller_index],
                                      self.decimals[controller_index])
                elif payload == b'031A':
                    self._send_packet(controller_address, b'%02X' % self.decimals[controller_index])
                elif payload.startswith(b'025C'):
                    payload = payload[4:]
                    if len(payload) != 6:
                        raise ValueError
                    if payload[:3] != b'000':
                        raise ValueError
                    if payload[4:] != b'00':
                        raise ValueError
                    decimals = int(payload[3:4], 16)
                    if decimals < 0 or decimals > 3:
                        raise ValueError
                    self.decimals[controller_index] = decimals
                    self._send_packet(controller_address, b'00')
                elif payload == b'0441' or payload == b'0400' or payload == b'032C':
                    self._send_packet(controller_address, b'00')
                elif payload == b'0408':
                    self.manual_mode[controller_index] = False
                    self._send_packet(controller_address, b'00')
                elif payload == b'0409':
                    self.manual_mode[controller_index] = True
                    self._send_packet(controller_address, b'00')
                elif payload == b'0700':
                    self._send_packet(controller_address, b'LOVE4213M32A')
                elif payload == b'0702':
                    self._send_packet(controller_address, b'501300')
                else:
                    _LOGGER.debug(f"Unsupported command payload {payload}")
                    raise ValueError
            except (ValueError, IndexError):
                self._send_error(controller_address, 0x01)
            await self.writer.drain()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
