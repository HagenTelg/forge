import typing
import asyncio
from forge.units import flow_lpm_to_ccs
from forge.acquisition.instrument.streaming import StreamingSimulator
from forge.acquisition.instrument.tsi3760cpc.instrument import Instrument


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.data_C1 = 1200.0
        self.data_C2 = 1300.0
        self.data_Q = Instrument.DEFAULT_FLOW
        self.delimiter = b"\r\x00"

    @property
    def data_N1(self) -> float:
        return self.data_C1 / flow_lpm_to_ccs(self.data_Q)

    @property
    def data_N2(self) -> float:
        return self.data_C2 / flow_lpm_to_ccs(self.data_Q)

    async def run(self) -> typing.NoReturn:
        while True:
            self.writer.write((
                f"{self.data_C1:8.0f} "
                f"{self.data_C2:8.0f}"
            ).encode('ascii'))
            self.writer.write(self.delimiter)

            await asyncio.sleep(1.0)


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
