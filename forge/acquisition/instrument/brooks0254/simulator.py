import typing
import asyncio
import logging
from forge.acquisition.instrument.streaming import StreamingSimulator

_LOGGER = logging.getLogger(__name__)


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.value: typing.List[float] = [float(i)+0.5 for i in range(4)]
        self.setpoint: typing.List[float] = [float(i)+0.75 for i in range(4)]

    @staticmethod
    def _checksum(data: typing.Iterable[int]) -> bytes:
        s = 0
        for a in data:
            s += a
            s &= 0xFF
        s = (~s + 1) & 0xFF
        return b'%02X' % s

    def _send_response(self, payload: bytes, port: int = None) -> None:
        address = 0
        if port:
            packet = b",%05d.%02d," % (address, port)
        else:
            packet = b",%05d," % address
        packet += payload
        packet += b","
        packet += self._checksum(packet)
        self.writer.write(b"AZ" + packet + b"\r\n")

    async def _read_command(self) -> typing.Tuple[bytes, typing.Optional[int]]:
        while True:
            line = await self.reader.readuntil(b'\r')
            line = line.strip()
            if line.endswith(b"\x1BAZ"):
                continue
            break

        if not line.startswith(b"AZ"):
            raise ValueError
        line = line[2:]
        if line[:1].isdigit():
            if len(line) < 5:
                raise ValueError
            address = line[:5]
            line = line[5:]
            if not address.isdigit():
                raise ValueError
        if line[:1] == b".":
            if len(line) < 3:
                raise ValueError
            port = int(line[1:3])
            if port <= 0:
                raise ValueError
            line = line[3:]
        else:
            port = None

        return line, port

    async def _wait_ack(self) -> None:
        (command, port) = await self._read_command()
        if command != b"A":
            raise ValueError
        if port is not None:
            raise ValueError

    async def run(self) -> typing.NoReturn:
        while True:
            (command, port) = await self._read_command()

            try:
                if command == b'S':
                    # XON, ignored
                    pass
                elif command == b"K" and port is not None:
                    if port <= 8 and (port % 2) == 1:
                        channel = port // 2
                        self._send_response(b"4,xxxxxxxx.xx,00162871.43,%11.2f,xxxxxxx.xx,xxxxx,X,X,X,X,X" % self.value[channel])
                    else:
                        raise ValueError
                elif command == b"P01?" and port is not None:
                    if port <= 8 and (port % 2) == 0:
                        channel = port // 2 - 1
                        self._send_response(b"4,P01,%11.2f" % self.setpoint[channel])
                    else:
                        raise ValueError
                elif command.startswith(b"P01=") and port is not None:
                    if port <= 8 and (port % 2) == 0:
                        channel = port // 2 - 1
                        value = float(command[4:])
                        self.setpoint[channel] = value
                        self._send_response(b"4,P01,%11.2f" % self.setpoint[channel])
                    else:
                        raise ValueError
                elif command == b"I" and port is None:
                    self._send_response(b"4,BROOKS,0254,08,01.01.13,FE00")
                elif command == b"V" and port is not None:
                    if port <= 8 and (port % 2) == 1:
                        self.writer.write((
                            f"PROGRAM VALUES - Channel {(port+1)//2} - Port {port:02d}\r\n\n"
                            "<04> Measure Units                 l\r\n"
                            "<10> Time Base                   min\r\n"
                            "<03> Decimal Point              x.xx\r\n"
                            "<27> Gas Factor                1.000\r\n"
                            "<28> Log Type                    Off\r\n"
                            "<00> PV Signal Type           0-20mA\r\n"
                            "<00> PV Full Scale         20.00 l/m\r\n"
                        ).encode('ascii'))
                    elif port <= 8 and (port % 2) == 0:
                        self.writer.write((
                            f"PROGRAM VALUES - Channel {port//2} - Port {port:02d}\r\n\n"
                            "<00> SP Signal Type          0-20mA\r\n"
                            "<09> SP Full Scale        20.00 l/m\r\n"
                            "<02> SP Function               Rate\r\n"
                            "<01> SP Rate               0.00 l/m\r\n"
                            "<29> SP VOR                  Normal\r\n"
                            "<44> SP Batch                0.00 l\r\n"
                            "<45> SP Blend               0.000 %\r\n"
                            "<46> SP Source               Keypad\r\n"
                        ).encode('ascii'))
                    elif port == 9:
                        self.writer.write((
                            "PROGRAM VALUES - Channel Global\r\n"
                            "\r\n\n"
                            "<39> Audio Beep                 On\r\n"
                            "<32> Zero Supress               On\r\n"
                            "<33> Pwr SP Clear              Off\r\n"
                            "<43> Record Count          0000000\r\n"
                            "<25> Sample Rate           535 sec\r\n"
                            "<22> Date-Time    00Jan00 00:00:00\r\n"
                            "<17> Network Addr            00000\r\n"
                        ).encode('ascii'))
                    else:
                        raise ValueError
                else:
                    raise ValueError
            except (ValueError, IndexError):
                pass
            await self.writer.drain()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
