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
        self.data_Q = 0.2
        self.data_C = self.data_N * self.data_Q * 1000.0
        self.data_P = 980.0
        self.data_Vpulse = 2123.0
        self.data_PCTwick = 95.0

        self.data_Tinlet = 21.0
        self.data_Tconditioner = 25.0
        self.data_Tinitiator = 30.0
        self.data_Tmoderator = 23.0
        self.data_Toptics = 22.0
        self.data_Theatsink = 55.0
        self.data_Tcase = 27.0
        self.data_Uinlet = 45.0
        self.data_TDinlet = 85.0
        self.data_TDgrowth = 85.0

        self.flags = 0

    async def _unpolled(self) -> typing.NoReturn:
        while True:
            ts = time.gmtime()
            self.writer.write((
                f"{ts.tm_year:04}/{ts.tm_mon:02}/{ts.tm_mday:02} {ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02},"
                f"{self.data_N:7.0f},"
                f"{self.data_TDinlet:4.1f},"
                f"{self.data_Tinlet:4.1f},"
                f"{self.data_Uinlet:4.1f},"
                f"{self.data_Tconditioner:4.1f},"
                f"{self.data_Tinitiator:4.1f},"
                f"{self.data_Tmoderator:4.1f},"
                f"{self.data_Toptics:4.1f},"
                f"{self.data_Theatsink:4.1f},"
                f"{self.data_Tcase:4.1f},"
                f"{self.data_PCTwick:6.0f},"
                "21.5,"
                f"{self.data_TDgrowth:4.1f},"
                f"{self.data_P:4.0f},"
                f"{self.data_Q*1000:3.0f},"
                "1,10000,    0,"
                f"{self.data_C:7.0f},"
                f"{self.data_Vpulse:5.0f}.50,"
                f"{self.flags:04X},"
                " ...... ,123\r"
            ).encode('ascii'))

            await asyncio.sleep(self.unpolled_interval)

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

    async def run(self) -> typing.NoReturn:
        try:
            await self._start_unpolled()
            while True:
                line = await self.reader.readuntil(b'\r')
                line = line.strip()

                # Echo
                self.writer.write(line)
                self.writer.write(b'\r\n')

                try:
                    if line.startswith(b'Log,'):
                        interval = int(line[4:])
                        if interval <= 0:
                            await self._stop_unpolled()
                        else:
                            self.unpolled_interval = interval
                            await self._start_unpolled()
                        self.writer.write(b'OK\r\n')
                    elif line == b'ver' or line == b'rv':
                        self.writer.write(b' Serial Number: 123\r\n')
                        self.writer.write(b' FW Ver: 3.03a\r\n')
                        self.writer.write(b' 2022 Aug 31\r\n')
                    elif line.startswith(b'rtc,'):
                        self.writer.write(b'\r\nOK\r\n')
                    elif line == b'wadc':
                        self.writer.write(b'59\r\n')
                    elif line == b'hdr':
                        self.writer.write(b'year time, Concentration, DewPoint,Input T, Input RH \r\n')
                        self.writer.write(b'Cond T, Init T,Mod T, Opt T, HeatSink T,  Case T,\r\n')
                        self.writer.write(b'wickSensor, ModSet, Humidifer Exit DP,\r\n')
                        self.writer.write(b'Abs. Press., flow (cc/min)\r\n')
                        self.writer.write(b'log interval, corrected live time, measured dead time, raw counts, PulseHeight.Thres2\r\n')
                        self.writer.write(b'Status(hex code), Status(ascii), Serial Number\r\n')
                    else:
                        raise ValueError
                except (ValueError, IndexError):
                    self.writer.write(b'ERROR in cmd\r')
                await self.writer.drain()
        finally:
            await self._stop_unpolled()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
