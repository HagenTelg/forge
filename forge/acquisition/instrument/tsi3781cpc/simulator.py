import typing
import asyncio
from forge.units import flow_lpm_to_ccm, flow_lpm_to_ccs
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.data_Q = 0.12
        self.data_C = 1200.0
        self.data_P = 880.0
        self.data_Tsaturator = 45.0
        self.data_Tgrowth = 20.0
        self.data_Toptics = 25.0
        self.data_Alaser = 15.0
        self.data_PCTnozzle = 100.0

        self.flags = 0

    @property
    def data_N(self) -> float:
        return self.data_C / flow_lpm_to_ccs(self.data_Q)

    async def run(self) -> typing.NoReturn:
        while True:
            line = await self.reader.readuntil(b'\r')
            line = line.strip()

            try:
                if line == b'RDD':
                    self.writer.write((
                        f"D,0,"
                        f"{self.flags:X},"
                        f"{self.data_N:.2e},"
                        f"1,1.000,"
                        f"{self.data_C:.0f},"
                        f"0,0\r"
                    ).encode('ascii'))
                elif line == b'RRS':
                    self.writer.write((
                        f"S,"
                        f"{flow_lpm_to_ccm(self.data_Q):.0f},"
                        f"{self.data_P:.0f},"
                        f"{self.data_Tsaturator:.1f},"
                        f"{self.data_Tgrowth:.1f},"
                        f"{self.data_Toptics:.1f}\r"
                    ).encode('ascii'))
                elif line == b'RL':
                    self.writer.write(f"{self.data_Alaser:.0f}\r".encode('ascii'))
                elif line == b'RN':
                    self.writer.write(f"{self.data_PCTnozzle:.0f}\r".encode('ascii'))
                elif line == b'RV':
                    self.writer.write(b"Model 3781 Ver 0.20 S/N 101\r")
                elif line.startswith(b'SM,'):
                    self.writer.write(b"OK\r")
                elif line.startswith(b'LM,'):
                    self.writer.write(b"OK\r")
                elif line.startswith(b'LC,'):
                    self.writer.write(b"OK\r")
                else:
                    raise ValueError
            except (ValueError, IndexError):
                self.writer.write(b'ERROR\r')
            await self.writer.drain()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
