import typing
import asyncio
from forge.units import flow_lpm_to_ccs, flow_lpm_to_ccm
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.data_Q = 1.0
        self.data_Qinlet = 1.5
        self.data_C = 1000.0
        self.data_P = 820.0
        self.data_PDnozzle = 20.0
        self.data_PDorifice = 50.0
        self.data_Alaser = 23.0
        self.data_liquid_level = 2345
        self.data_Tsaturator = 45.0
        self.data_Tcondenser = 15.0
        self.data_Toptics = 25.0
        self.data_Tcabinet = 21.0

        self.flags: int = 0

        self.support_scd: bool = True
        self.not_full: bool = False

    @property
    def data_N(self) -> float:
        return self.data_C / flow_lpm_to_ccs(self.data_Q)

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
                        f"{self.not_full and 'NOT' or ''}FULL ({self.data_liquid_level})\r"
                    ).encode('ascii'))
                elif line == b'RSF':
                    self.writer.write(f"{flow_lpm_to_ccm(self.data_Q):.1f}\r".encode('ascii'))
                elif line == b'RIF':
                    self.writer.write(f"{self.data_Qinlet:.1f}\r".encode('ascii'))
                elif line == b'RCOUNT1' or line == b'RCOUNT2':
                    self.writer.write(f"{int(self.data_C):d}\r".encode('ascii'))
                elif line == b'RMN':
                    self.writer.write(b"3775\r")
                elif line == b'RSN':
                    self.writer.write(b"70514396\r")
                elif line == b'RFV':
                    self.writer.write(b"2.3.1\r")
                elif line == b'SCD' and self.support_scd:
                    self.writer.write(b"020313,010313,1\r")
                elif line == b'RIE':
                    self.writer.write(f"{self.flags:X}\r".encode('ascii'))
                elif line.startswith(b'SCM,') or line.startswith(b'SSTART,') or line.startswith(b'SCC,'):
                    self.writer.write(b"OK\r")
                else:
                    raise ValueError
            except (ValueError, IndexError):
                self.writer.write(b'ERROR\r')
            await self.writer.drain()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
