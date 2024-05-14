import typing
import asyncio
import time
from forge.units import flow_lpm_to_ccm
from forge.tasks import wait_cancelable
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.unpolled_interval = 2.0
        self._unpolled_task: typing.Optional[asyncio.Task] = None

        self.data_X = 39.0
        self.data_T = 35.0
        self.data_P = 801.5
        self.data_Q = 2.230

    async def _unpolled(self) -> typing.NoReturn:
        while True:
            ts = time.gmtime()
            self.writer.write((
                f"{self.data_X:.1f},"
                f"{self.data_T:.1f},"
                f"{self.data_P:.1f},"
                f"{flow_lpm_to_ccm(self.data_Q):.0f},"
                f"{ts.tm_mday:02}/{ts.tm_mon:02}/{ts.tm_year % 100:02},{ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}"
                "\r"
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
                try:
                    await wait_cancelable(self.reader.readuntil(b'm'), 1.0)
                except asyncio.TimeoutError:
                    continue
                await self._stop_unpolled()
                await asyncio.sleep(self.unpolled_interval)

                self.writer.write(b'Press ? for help\r\n')
                while True:
                    self.writer.write(b'menu>')
                    await self.writer.drain()

                    code = await self.reader.readexactly(1)
                    self.writer.write(code)
                    self.writer.write(b"\r\n")
                    await self.writer.drain()

                    if code == b'x':
                        self.writer.write(b'Exiting menu\r\n')
                        await self.writer.drain()
                        await asyncio.sleep(max(self.unpolled_interval, 35))
                        await self._start_unpolled()
                        break
                    elif code == b'e':
                        pass
                    elif code == b'v':
                        self.writer.write(b'Stopped displaying raw ozone for channel A and B for each cycle.\r\n')
                    elif code == b'n':
                        pass
                    elif code == b'a':
                        self.writer.write(b'Enter Average setting (0 = 2 second, 1 = 10 second, 2 = 1 minute, 3 = 5 minute, 4 = 1 hour):')
                        await self.writer.drain()
                        code = await self.reader.readexactly(1)
                        self.writer.write(code)
                        self.writer.write(b"\r\n")
                        await self.writer.drain()
                        if code == b'0':
                            self.unpolled_interval = 2.0
                            self.writer.write(b'Avg: 2 s/rdg\r\n')
                        elif code == b'1':
                            self.unpolled_interval = 10.0
                            self.writer.write(b'Avg: 10 s/rdg\r\n')
                        elif code == b'2':
                            self.unpolled_interval = 60.0
                            self.writer.write(b'Avg: 1 min/rdg\r\n')
                        elif code == b'3':
                            self.unpolled_interval = 300.0
                            self.writer.write(b'Avg: 5 min/rdg\r\n')
                        elif code == b'4':
                            self.unpolled_interval = 3600.0
                            self.writer.write(b'Avg: 1 hour/rdg\r\n')
                        else:
                            self.writer.write(b'ERROR: Invalid averaging\r\n')
                    elif code == b'c':
                        ts = time.gmtime()
                        self.writer.write((
                          "Current Date and Time:\r\n"
                          f"{ts.tm_mday:02}/{ts.tm_mon:02}/{ts.tm_year % 100:02}\r\n"
                          f"{ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}\r\n"
                          "Change Date(d) or Time(t)?"
                        ).encode('ascii'))
                        await self.writer.drain()
                        code = await self.reader.readexactly(1)
                        self.writer.write(code)
                        self.writer.write(b"\r\n")
                        await self.writer.drain()
                        if code == b'd' or code == b't':
                            digits = await self.reader.readexactly(6)
                            self.writer.write(digits)
                            self.writer.write(b"\r\n")
                            if not digits.isdigit():
                                self.writer.write(b'ERROR: Datetime setting\r\n')

        finally:
            await self._stop_unpolled()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
