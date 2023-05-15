import typing
import asyncio
from forge.units import flow_lpm_to_ccs
from forge.acquisition.instrument.streaming import StreamingSimulator
from forge.acquisition.instrument.tsi3010cpc.instrument import Instrument


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.data_Q = Instrument.DEFAULT_FLOW
        self.data_C = 1000.0
        self.data_Tsaturator = 45.0
        self.data_Tcondenser = 15.0

    @property
    def data_N(self) -> float:
        return self.data_C / flow_lpm_to_ccs(self.data_Q)

    async def run(self) -> typing.NoReturn:
        while True:
            line = await self.reader.readuntil(b'\r')
            line = line.strip()

            try:
                if line == b'DC':
                    self.writer.write(f"1,{self.data_C:.0f}\r".encode('ascii'))
                elif line == b'R0':
                    self.writer.write(b"FULL\r")
                elif line == b'R1':
                    self.writer.write(f"{self.data_Tcondenser:.1f}\r".encode('ascii'))
                elif line == b'R2':
                    self.writer.write(f"{self.data_Tsaturator:.1f}\r".encode('ascii'))
                elif line == b'R5':
                    self.writer.write(b"READY\r")
                elif line == b'RV':
                    self.writer.write(b"VAC\r")
                elif line == b'X5':
                    self.writer.write(b"OK\r")
                else:
                    raise ValueError
            except (ValueError, IndexError):
                self.writer.write(b'ERROR\r')
            await self.writer.drain()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
