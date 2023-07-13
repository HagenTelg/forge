import typing
import asyncio
import time
from math import log
from forge.units import flow_lpm_to_m3s
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.model_run_time: float = 20.0
        self.unpolled_delay: float = 2.0
        self._unpolled_task: typing.Optional[asyncio.Task] = None

        self._output_buffer = bytearray()

        self.data_Ba = 1.5
        self.data_X = 25.0
        self.data_If = 1020.0
        self.data_Ip = 1021.0
        self.data_Is135 = 1022.0
        self.data_Is165 = 1024.0
        self.data_If0 = 1120.0
        self.data_Ip0 = 1121.0
        self.data_Is0135 = 1122.0
        self.data_Is0165 = 1124.0
        self.data_SSA = 0.994
        self.data_Tsample = 21.0
        self.data_Thead = 22.0
        self.data_Tsystem = 23.0
        self.data_P = 880.0
        self.data_PDorifice = 100.0
        self.data_PDvacuum = 101.0
        self.data_Q = 12.0
        self.data_PCT = 50.0

    @property
    def data_Ir(self) -> float:
        return (self.data_Ip / self.data_If) / (self.data_Ip0 / self.data_If0)

    @property
    def data_Bac(self) -> float:
        return self.data_X * 6.6

    @property
    def data_X_uncorrected(self) -> float:
        return self.data_Ba / 6.6

    async def _flush_buffer(self) -> None:
        self.writer.write(self._output_buffer)
        self.writer.write(b"\r\n")
        self._output_buffer.clear()
        await self.writer.drain()

    async def _unpolled(self) -> typing.NoReturn:
        while True:
            ts = time.gmtime()
            self._output_buffer += ((
                f"{ts.tm_year % 100:02}-{ts.tm_mon:02}-{ts.tm_mday:02} {ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02} "
                f"{self.data_Ip:.2f} "
                f"{self.data_Is135:.2f} "
                f"{self.data_Is165:.2f} "
                f"{self.data_If:.2f} "
                f"{self.data_Ip0:.2f} "
                f"{self.data_Is0135:.2f} "
                f"{self.data_Is0165:.2f} "
                f"{self.data_If0:.2f} "
                f"{flow_lpm_to_m3s(self.data_Q) * (self.model_run_time + self.unpolled_delay):.3f} "
            ).encode('ascii'))

            await asyncio.sleep(self.model_run_time)

            self._output_buffer += ((
                "  17 "
                f"{self.data_SSA:.6f} "
                f"{100.0 * log(self.data_Ir):.3f} "
                "0.93 "
                f"{self.data_X:.2f} "
                f"{self.data_X_uncorrected:.2f}"
            ).encode('ascii'))

            await self._flush_buffer()
            await asyncio.sleep(self.unpolled_delay)

    async def _stop_unpolled(self) -> None:
        t = self._unpolled_task
        self._unpolled_task = None
        if not t:
            return
        try:
            t.cancel()
        except:
            pass
        try:
            await t
        except:
            pass

    async def _start_unpolled(self) -> None:
        await self._stop_unpolled()
        self._unpolled_task = asyncio.ensure_future(self._unpolled())

    async def _output_and_flush(self, contents: bytes) -> None:
        self._output_buffer += contents
        await self._flush_buffer()

    async def run(self) -> typing.NoReturn:
        async def output_number(value: float) -> None:
            self._output_buffer += b" %.2f" % value
            await self._flush_buffer()

        async def output_integer(value: int) -> None:
            self._output_buffer += b" %d" % value
            await self._flush_buffer()

        error_counter: int = 0
        will_read_parameters: bool = False
        try:
            await self._start_unpolled()
            while True:
                line = await self.reader.readuntil(b'\r')
                line = line.strip()

                try:
                    if line == b'v':
                        self._output_buffer += b"SERIAL NUMBER    89\r\nTHERMO SCIENTIFIC  MAAP v1.32"
                        await self._flush_buffer()
                    elif line == b'N':
                        await output_integer(error_counter)
                        error_counter = 0
                    elif line == b'J0':
                        await output_number(self.data_Tsample)
                    elif line == b'J1':
                        await output_number(self.data_Thead)
                    elif line == b'J2':
                        await output_number(self.data_Tsystem)
                    elif line == b'J3':
                        await output_number(self.data_PDorifice)
                    elif line == b'J4':
                        await output_number(self.data_PDvacuum)
                    elif line == b'J5':
                        await output_number(self.data_P)
                    elif line == b'JK':
                        await output_number(self.data_Q * 60.0)
                    elif line == b'JM':
                        await output_integer(round(self.data_PCT * 4096.0 / 100.0))
                    elif line == b'#':
                        self._output_buffer += b" 0000 0000 0000 0000 000000"
                        await self._flush_buffer()
                    elif line == b'?':
                        await output_integer(1)
                    elif line == b'D 0':
                        will_read_parameters = False
                        await self._stop_unpolled()
                    elif line == b'D 12':
                        will_read_parameters = False
                        await self._start_unpolled()
                    elif line == b'D 8':
                        will_read_parameters = True
                    elif line == b'P' and will_read_parameters:
                        self._output_buffer += (
                            b"  THERMO SCIENTIFIC  MAAP v1.32         SERIAL NUMBER  89    14-02-19\r\n"
                            b"--------------------------------------------------------------------------------\r\n"
                            b"SIGMA BC:              6.6 m2/g\r\n"
                            b"AIR FLOW:             1000\r\n"
                            b"STORE AVERAGES:          0 min \r\n"
                            b"VOLUME REFERENCE    STANDARD TEMPERATURE\r\n"
                            b"STANDARD TEMPERATURE     0 _C  \r\n"
                            b"PRINTFORMAT:        COM2     0\r\n"
                            b"PRINTCYCLE:              0 s   \r\n"
                            b"BAUDRATE:        Bd COM1  9600\r\n"
                            b"BAUDRATE:        Bd COM2  9600\r\n"
                            b"DEVICE-ADDRESS:          1\r\n"
                            b"FILTER CHANGE       \r\n"
                            b"TRANSM. <       %       70\r\n"
                            b"CYCLE           h      100\r\n"
                            b"HOUR:                    0\r\n"
                            b"CALIBRATION OF SENS.\r\n"
                            b"    T1    T2    T3    T4    P1    P2    P3\r\n"
                            b"   -33   -21   -41    65     6    76    44\r\n"
                            b"AIR FLOW             100.1\r\n"
                            b"HEATER PARAMETERS   \r\n"
                            b"Diff. T2-T1 nominal      0 _C  \r\n"
                            b"Max. Heating Temp.      45 _C  \r\n"
                            b"Min. Heating Power      10 %   \r\n"
                            b"ANALOG OUTPUTS      \r\n"
                            b"OUTPUT ZERO:         4mA \r\n"
                            b"CBC      0    10\r\n"
                            b"MBC      0  2400\r\n"
                            b"GESYTEC-PROTOKOL    \r\n"
                            b"STATUS VERSION      STANDARD \r\n"
                            b"NUMBER OF VARIABLES      1\r\n"
                            b"CBC \r\n"
                            b"END"
                        )
                        await self._flush_buffer()
                    elif line.startswith(b"KK "):
                        pass
                    elif line.startswith(b"KN "):
                        pass
                    elif line.startswith(b"KM "):
                        pass
                    elif line.startswith(b"K0 "):
                        pass
                    elif line.startswith(b"K1 "):
                        pass
                    elif line.startswith(b"K2 "):
                        pass
                    elif line.startswith(b"K3 "):
                        pass
                    elif line.startswith(b"Z"):
                        pass
                    elif line.startswith(b"d2 "):
                        pass
                    elif line.startswith(b"d3 "):
                        pass
                    elif line == b"F":
                        pass
                    else:
                        raise ValueError
                except (ValueError, IndexError):
                    self.writer.write(b'ERROR\r\n')
                    error_counter += 1
                await self.writer.drain()
        finally:
            await self._stop_unpolled()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
