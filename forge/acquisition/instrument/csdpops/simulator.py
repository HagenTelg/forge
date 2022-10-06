import typing
import asyncio
import time
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.unpolled_interval = 1.0
        self._unpolled_task: typing.Optional[asyncio.Task] = None

        self.data_N = 1234.0
        self.data_Q = 0.15
        self.data_C = self.data_N * self.data_Q * 1000.0 / 60.0
        self.data_baseline = 10
        self.data_baseline_threshold = 11
        self.data_baseline_stddev = 12
        self.data_baseline_stddevmax = 13
        self.data_P = 980.0
        self.data_Tpressure = 21.0
        self.data_pump_on_time = 15
        self.data_peak_width = 16
        self.data_pump_feedback = 17
        self.data_Tlaser = 22.0
        self.data_laser_feedback = 18
        self.data_laser_monitor = 19
        self.data_Tinternal = 23.0
        self.data_Vsupply = 12.5
        self.data_Alaser = 2.25

        self.data_Cb = []
        self.data_dN = []
        ccs = self.data_Q * 1000.0 / 60.0
        for i in range(10):
            Cb = (i + 1) * 5
            self.data_Cb.append(Cb)
            self.data_dN.append(Cb / ccs)

        self.flags = 0

    async def run(self) -> None:
        while True:
            ts = time.gmtime()
            self.writer.write((
                "POPS,POPS_H0086,/media/uSD/Data/F20200220/Peak_20200220x003.b,"
                f"{ts.tm_year:04}{ts.tm_mon:02}{ts.tm_mday:02}T{ts.tm_hour:02}{ts.tm_min:02}{ts.tm_sec:02},"
                "56936.4312,3,"
                f"{self.flags},"
                f"{self.data_C},"
                f"{sum(self.data_Cb)},"
                f"{self.data_N},"
                f"{self.data_baseline},"
                f"{self.data_baseline_threshold},"
                f"{self.data_baseline_stddev},"
                f"{self.data_baseline_stddevmax},"
                f"{self.data_P},"
                f"{self.data_Tpressure},"
                f"{self.data_pump_on_time},"
                "0,"
                f"{self.data_peak_width},"
                f"{self.data_Q * 1000.0 / 60.0},"
                f"{self.data_pump_feedback},"
                f"{self.data_Tlaser},"
                f"{self.data_laser_feedback},"
                f"{self.data_laser_monitor},"
                f"{self.data_Tinternal},"
                f"{self.data_Vsupply},"
                f"{self.data_Alaser},"
                "2.98,30000,3,"
                f"{len(self.data_Cb)},"
                "1.75,4.81,0,8,255,512,"
            ).encode('ascii'))
            self.writer.write((
                ",".join([f"{c}" for c in self.data_Cb])
            ).encode('ascii'))
            self.writer.write(b"\r\n")

            await asyncio.sleep(self.unpolled_interval)


if __name__ == '__main__':
    import sys
    from forge.acquisition.serial.simulator import run
    run(sys.argv[1], Simulator)
