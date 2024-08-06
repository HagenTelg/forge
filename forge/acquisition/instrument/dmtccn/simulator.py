import typing
import asyncio
import time
from forge.units import flow_lpm_to_ccs, flow_lpm_to_ccm
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.unpolled_interval = 2.0
        self._unpolled_task: typing.Optional[asyncio.Task] = None

        self.data_N = 1234.0
        self.data_Ttec1 = 30.0
        self.data_Ttec2 = 40.0
        self.data_Ttec3 = 50.0
        self.data_Tsample = 25.0
        self.data_Topc = 26.0
        self.data_Tinlet = 21.0
        self.data_Tnafion = 22.0
        self.data_DTsetpoint = 29.0
        self.data_Q = 1.5
        self.data_Qsheath = 2.0
        self.data_SSset = 2.5
        self.data_P = 800.0
        self.data_Vmonitor = 0.5
        self.data_Vvalve = 0.75
        self.data_Vvalve = 0.75
        self.data_Alaser = 15.0
        self.data_minimum_bin_number = 1

        self.data_Cb = []
        for i in range(21):
            self.data_Cb.append((i + 1) * 5)

        self.alarm = 0

        self.record_join: bytes = b"\r"
        self.alarm_record: str = "H"

    @property
    def data_dN(self) -> typing.List[float]:
        ccs = flow_lpm_to_ccs(self.data_Q)
        return [c / ccs for c in self.data_Cb]

    async def run(self) -> typing.NoReturn:
        while True:
            ts = time.gmtime()
            self.writer.write((
                "H,"
                f"{ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02},"
                f"{self.data_SSset:.2f},"
                "1.00,"
                f"{self.data_Ttec1:.2f},"
                f"{self.data_Ttec2:.2f},"
                f"{self.data_Ttec3:.2f},"
                f"{self.data_Tsample:.2f},"
                f"{self.data_Tinlet:.2f},"
                f"{self.data_Topc:.2f},"
                f"{self.data_Tnafion:.2f},"
                f"{flow_lpm_to_ccm(self.data_Q):.2f},"
                f"{flow_lpm_to_ccm(self.data_Qsheath):.2f},"
                f"{self.data_P:.2f},"
                f"{self.data_Alaser:.2f},"
                f"{self.data_Vmonitor:.2f},"
                f"{self.data_DTsetpoint:.2f},"
                f"{self.data_Vvalve:.2f}"
                + (f",{self.alarm}" if self.alarm_record == "H" else "")
            ).encode('ascii'))
            self.writer.write(self.record_join)
            self.writer.write((
                "C,"
                f"{self.data_minimum_bin_number:.2f},"
                f"{self.data_N:.2f},"
                f"{self.data_Cb[20]:.2f},"
            ).encode('ascii'))
            self.writer.write((
                ",".join([f"{c:.2f}" for c in self.data_Cb[:20]])
            ).encode('ascii'))
            if self.alarm_record == "C":
                self.writer.write(f",{self.alarm}".encode('ascii'))
            self.writer.write(b"\r")

            await asyncio.sleep(self.unpolled_interval)


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
