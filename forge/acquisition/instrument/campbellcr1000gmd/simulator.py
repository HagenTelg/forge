import typing
import asyncio
from math import isfinite
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.ain: typing.List[float] = [float(i) for i in range(32)]
        self.aot: typing.List[float] = [0.0] * 8
        self.dot: int = 0

        self.data_T = 21.5
        self.data_V = 12.5

    async def run(self) -> typing.NoReturn:
        while True:
            line = await self.reader.readuntil(b'\r')
            line = line.strip()

            try:
                if line == b'RST':
                    self.writer.write(f'STA,{self.dot:08X}'.encode('ascii'))
                    for v in self.ain:
                        if not isfinite(v):
                            self.writer.write(b',NAN')
                            continue
                        self.writer.write(f',{v:.7f}'.encode('ascii'))
                    self.writer.write(f',{self.data_V:.7f},{self.data_T:.7f}\r'.encode('ascii'))
                elif line.startswith(b'SDO,'):
                    digital = int(line[4:], 16)
                    self.dot = digital
                    self.writer.write(f'SDA,{self.dot:08X}\r'.encode('ascii'))
                elif line.startswith(b'SAO,'):
                    (_, index, value) = line.split(b',')
                    index = int(index)
                    value = float(value)
                    self.aot[index] = value
                    self.writer.write(f'SAA,{index},{value:.7f}\r'.encode('ascii'))
                else:
                    raise ValueError
            except (ValueError, IndexError):
                self.writer.write(b'ERR,Invalid command\r')
            await self.writer.drain()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
