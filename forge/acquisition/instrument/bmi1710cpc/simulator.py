import typing
import asyncio
import time
from forge.units import flow_lpm_to_ccm
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.unpolled_interval = 1.0
        self._unpolled_task: typing.Optional[asyncio.Task] = None

        self.data_N = 1234.0
        self.data_C = 2000.0
        self.data_Q = 0.5
        self.data_Qsaturator = 1.25

        self.data_Toptics = 25.0
        self.data_Tcondenser = 18.0
        self.data_Tsaturatortop = 40.0
        self.data_Tsaturatorbottom = 41.0
        self.data_Tinlet = 21.0

        self.data_PCToptics = 75.0
        self.data_PCTcondenser = 50.0
        self.data_PCTsaturatortop = 40.0
        self.data_PCTsaturatorbottom = 30.0
        self.data_PCTsaturatorpump = 60.0

        self.flags = 0

    def _report(self) -> None:
        self.writer.write((
            f"CONCN={self.data_N:.1f} "
            f"RCONC={self.data_N + 2.0:.1f} "
            f"COUNT={self.data_C:.0f} "
            f"OPTCT={self.data_Toptics:.1f} "
            f"OPTCP={self.data_PCToptics / 100.0 * 200.0:.0f}\r\n"
            f"CONDT={self.data_Tcondenser:.1f} "
            f"CONDP={self.data_PCTcondenser / 100.0 * 250.0:.0f} "
            f"SATTT={self.data_Tsaturatortop:.1f} "
            f"SATTP={self.data_PCTsaturatortop / 100.0 * 200.0:.0f} "
            f"SATBT={self.data_Tsaturatorbottom:.1f} "
            f"SATBP={self.data_PCTsaturatorbottom / 100.0 * 200.0:.0f}\r\n"
            f"SATFL={flow_lpm_to_ccm(self.data_Qsaturator):.0f} "
            f"SATFP={self.data_PCTsaturatorpump / 100.0 * 200.0:.0f} "
            f"SMPFL={flow_lpm_to_ccm(self.data_Q):.0f} "
            f"INLTT={self.data_Tinlet:.1f} "
            f"FILLC=5 "
            f"ERRNM={self.flags:04X}\r\n"
        ).encode('ascii'))

    async def _unpolled(self) -> typing.NoReturn:
        while True:
            self._report()
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

                try:
                    if line.startswith(b'autorpt='):
                        enable = int(line[8:])
                        await self._stop_unpolled()
                        if enable:
                            await self._start_unpolled()
                    elif line.startswith(b'rptlabel='):
                        self.writer.write(b'OK\r\n')
                    elif line == b'ver':
                        self.writer.write(b'BMI MCPC v3.4\r\n')
                    elif line == b'status':
                        self._report()
                    else:
                        raise ValueError
                except (ValueError, IndexError):
                    self.writer.write(b'ERROR\r')
                await self.writer.drain()
        finally:
            await self._stop_unpolled()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
