import typing
import asyncio
from forge.units import flow_lpm_to_ccs, flow_lpm_to_ccm
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.data_N = 1234.0
        self.data_Q = 1.0
        self.data_Qinlet = 1.5
        self.data_P = 820.0
        self.data_PDnozzle = 20.0
        self.data_PDorifice = 50.0
        self.data_Alaser = 23.0
        self.data_liquid_level = 2345
        self.data_Vphoto = 0.75
        self.data_Tsaturator = 45.0
        self.data_Tcondenser = 15.0
        self.data_Toptics = 25.0
        self.data_Tcabinet = 21.0

        self.flags: int = 0

        self.model_number: str = "3755"

    async def run(self) -> typing.NoReturn:
        while True:
            line = await self.reader.readuntil(b'\r')
            line = line.strip()

            try:
                if line == b'RALL':
                    self.writer.write((
                        f"{self.data_N:.1f},"
                        f"{self.flags:04X},"
                        f"{self.data_Tsaturator:.1f},"
                        f"{self.data_Tcondenser:.1f},"
                        f"{self.data_Toptics:.1f},"
                        f"{self.data_Tcabinet:.1f},"
                        f"{self.data_P / 10.0:.1f},"
                        f"{self.data_PDorifice / 10.0:.1f},"
                        f"{self.data_PDnozzle / 10.0:.1f},"
                        f"{self.data_Alaser:.0f},"
                        f"FULL ({self.data_liquid_level})\r"
                    ).encode('ascii'))
                elif line == b'RSF':
                    self.writer.write(f"{flow_lpm_to_ccm(self.data_Q):.1f}\r".encode('ascii'))
                elif line == b'RIF':
                    self.writer.write(f"{self.data_Qinlet:.1f}\r".encode('ascii'))
                elif line == b'R7':
                    self.writer.write(f"{self.data_Vphoto:.3f}\r".encode('ascii'))
                elif line == b'RMN':
                    self.writer.write(f"{self.model_number}\r".encode('ascii'))
                elif line == b'RSN':
                    self.writer.write(b"70514396\r")
                elif line == b'RV':
                    self.writer.write(f"Model {self.model_number} Ver 0.20 S/N 101\r".encode('ascii'))
                elif line == b'RIE':
                    self.writer.write(f"{self.flags:X}\r".encode('ascii'))
                elif line.startswith(b'SCC,'):
                    self.writer.write(b"OK\r")
                else:
                    raise ValueError
            except (ValueError, IndexError):
                self.writer.write(b'ERROR\r')
            await self.writer.drain()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
