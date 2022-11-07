import typing
import asyncio
import time
import struct
import crc
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self._sequence_number: int = 0
        self._crc = crc.CrcCalculator(crc.Configuration(
            width=16,
            polynomial=0x8005,
            init_value=0,
            reverse_input=True,
            reverse_output=True,
        ), True)

        self.address = 0

        self.ain: typing.List[float] = [float(i) for i in range(24)]
        self.aot: typing.List[float] = [0.0] * 10
        self.dot: int = 0

        self.data_T = 21.0
        self.data_V = 5.0

    async def _read_command(self) -> typing.Tuple[int, bytes]:
        header = await self.reader.readexactly(6)
        adr, utp, bcs, pkt, cmd = struct.unpack('<BBHBB', header)

        packet_length = bcs & 0x3FF
        if packet_length < 4:
            raise ValueError
        self._sequence_number = (bcs >> 10) & 0x3F
        if packet_length > 4:
            payload = await self.reader.readexactly(packet_length - 4)
        else:
            payload = bytes()

        received_crc = await self.reader.readexactly(2)
        received_crc = struct.unpack('<H', received_crc)[0]
        expected_crc = self._crc.calculate_checksum(header + payload)
        if received_crc != expected_crc:
            raise ValueError

        if adr != self.address:
            raise ValueError
        if utp != 1:
            raise ValueError
        if pkt != 0:
            raise ValueError

        return cmd, payload

    async def _send_response(self, payload: bytes = None) -> None:
        bcs = (payload and len(payload) or 0) + 4
        bcs |= (self._sequence_number << 10)
        frame = struct.pack(
            '<BBHBB',
            self.address,   # ADR
            1,              # UTP
            bcs,            # BCS
            1,              # PKT (1 = normal)
            0               # STA
        )
        if payload:
            frame = frame + payload
        self.writer.write(frame)
        frame_crc = self._crc.calculate_checksum(frame)
        self.writer.write(struct.pack('<H', frame_crc))
        await self.writer.drain()

    async def _send_error(self, error_code: int) -> None:
        bcs = 6 | (self._sequence_number << 10)
        frame = struct.pack(
            '<BBHBBH',
            self.address,   # ADR
            1,              # UTP
            bcs,            # BCS
            3,              # PKT (3 = error)
            (1 << 7),       # STA
            error_code
        )
        self.writer.write(frame)
        frame_crc = self._crc.calculate_checksum(frame)
        self.writer.write(struct.pack('<H', frame_crc))
        await self.writer.drain()

    async def run(self) -> typing.NoReturn:
        while True:
            try:
                command, payload = await self._read_command()

                if command == 9:  # AIN
                    first_channel, last_channel = struct.unpack('<BB', payload)
                    count = (last_channel - first_channel) + 1
                    if count <= 0:
                        raise ValueError

                    if first_channel == 60 and last_channel == 60:
                        await self._send_response(struct.pack('<f', self.data_T))
                        continue
                    elif first_channel == 61 and last_channel == 61:
                        await self._send_response(struct.pack('<f', self.data_V))
                        continue
                    elif first_channel == 60 and last_channel == 61:
                        await self._send_response(struct.pack('<ff', self.data_T, self.data_V))
                        continue

                    if first_channel < 0 or last_channel >= len(self.ain):
                        raise ValueError
                    await self._send_response(struct.pack(
                        '<' + str(count) + 'f',
                        *self.ain[first_channel:(last_channel+1)]
                    ))
                elif command == 34:  # AOT
                    first_channel, last_channel, value = struct.unpack('<BBf', payload)
                    if first_channel < 0 or last_channel >= len(self.aot):
                        raise ValueError
                    for i in range(first_channel, last_channel+1):
                        self.aot[i] = value
                    await self._send_response()
                elif command == 50:  # DOT
                    first_channel, last_channel, mask, value = struct.unpack('<BBBB', payload)
                    if first_channel < 0 or last_channel > 63:
                        raise ValueError
                    for i in range(first_channel, last_channel + 1):
                        effective_mask = mask << (i * 8)
                        effective_value = value << (i * 8)
                        self.dot &= ~effective_mask
                        self.dot |= (effective_value & effective_mask)
                    await self._send_response()
                elif command == 161:  # CNFGLD
                    await self._send_response()
                elif command == 177:  # RESET
                    await self._send_response()
                elif command == 180:  # REV
                    await self._send_response(b'uMAC-1050 Firmware Rev. 1.00 (c)1989')
                else:
                    raise ValueError
            except (ValueError, IndexError, struct.error):
                await self._send_error(1)
            await self.writer.drain()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
