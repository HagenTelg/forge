import typing
import asyncio
import time
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.data_Q = 1.5
        self.data_T = 20.0
        self.data_P = 890.0
        self.data_U = 30.0

        self.model_number = b"5000"

        self._sample_time: float = 1.0

    async def run(self) -> typing.NoReturn:
        while True:
            line = await self.reader.readuntil(b'\r')
            line = line.strip()

            try:
                if line == b'CBT':
                    self.writer.write(b'OK\r\n')
                elif line == b'CET':
                    self.writer.write(b'OK\r\n')
                elif line == b'SUS':
                    self.writer.write(b'OK\r\n')
                elif line == b'BREAK':
                    self.writer.write(b'OK\r\n')
                elif line == b'SN':
                    self.writer.write(b'OK\r\n50409806004\r\n')
                elif line == b'MN':
                    self.writer.write(b'OK\r\n' + self.model_number + b'\r\n')
                elif line == b'REV':
                    self.writer.write(b'OK\r\n1.3\r\n')
                elif line == b'DATE':
                    self.writer.write(b'OK\r\n12/24/98\r\n')
                elif line.startswith(b'SSR'):
                    ms = int(line[3:])
                    if ms < 1 or ms > 1000:
                        raise ValueError
                    self._sample_time = ms / 1000.0
                    self.writer.write(b'OK\r\n')
                elif line.startswith(b'DAFTPHxx') and len(line) == 12:
                    count = int(line[8:])
                    if count < 1 or count > 1000:
                        raise ValueError
                    self.writer.write(b'OK\r\n')
                    for _ in range(count):
                        await asyncio.sleep(self._sample_time)
                        self.writer.write((
                            f"{self.data_Q * ((273.15 + 21.11) / 273.15) * (1013.25 / 1013.0):.2f},"
                            f"{self.data_T:.2f},"
                            f"{self.data_P / 10.0:.3f},"
                            f"{self.data_U:.3f}\r\n"
                        ).encode('ascii'))
                elif line.startswith(b'DAFTP') and len(line) == 9:
                    count = int(line[5:])
                    if count < 1 or count > 1000:
                        raise ValueError
                    self.writer.write(b'OK\r\n')
                    for _ in range(count):
                        await asyncio.sleep(self._sample_time)
                        self.writer.write((
                            f"{self.data_Q * ((273.15 + 21.11) / 273.15) * (1013.25 / 1013.0):.2f},"
                            f"{self.data_T:.2f},"
                            f"{self.data_P / 10.0:.3f}\r\n"
                        ).encode('ascii'))
                else:
                    raise ValueError
            except (ValueError, IndexError):
                self.writer.write(b'ERR1\r\n')
            await self.writer.drain()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
