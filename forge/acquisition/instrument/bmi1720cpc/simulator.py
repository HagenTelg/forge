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
        self.data_P = 880.0
        self.data_RAWP = 90.0
        self.data_RAWQsample = 91.0
        self.data_RAWQsaturator = 92.0

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
            f"concent={self.data_N:.1f}\r"            
            f"rawconc=2000\r"
            f"cnt_sec={self.data_C:.0f}\r"
            f"condtmp={self.data_Tcondenser:.1f}\r"
            f"satttmp={self.data_Tsaturatortop:.1f}\r"
            f"satbtmp={self.data_Tsaturatorbottom:.1f}\r"
            f"optctmp={self.data_Toptics:.1f}\r"
            f"inlttmp={self.data_Tinlet:.1f}\r"
            f"smpflow={flow_lpm_to_ccm(self.data_Q):.0f}\r"
            f"satflow={flow_lpm_to_ccm(self.data_Qsaturator):.0f}\r"
            f"pressur={self.data_P:.0f}\r"
            f"condpwr={self.data_PCTcondenser / 100.0 * 250.0:.0f}\r"
            f"sattpwr={self.data_PCTsaturatortop / 100.0 * 200.0:.0f}\r"
            f"satbpwr={self.data_PCTsaturatorbottom / 100.0 * 200.0:.0f}\r"
            f"optcpwr={self.data_PCToptics / 100.0 * 200.0:.0f}\r"
            f"satfpwr={self.data_PCTsaturatorpump / 100.0 * 200.0:.0f}\r"
            f"fillcnt=5\r"
            f"err_num={self.flags}\r\r"
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
                    elif line == b'settings':
                        self.writer.write(
                            b'autorpt=0\r'
                            b'rpt_lbl=1\r'
                            b'sd_stat=0\r'
                            b'sd_save=0\r'
                            b'sd_intv=60\r'
                            b'sd_file=N/A\r'
                            b'sd_size=N/A\r'
                            b'sd_used=N/A\r\r'
                        )
                    elif line == b'calib':
                        self.writer.write(
                            b'smpslop=100\r'
                            b'smpoffs=50\r'
                            b'satslop=110\r'
                            b'satoffs=60\r'
                            b'cal_tmp=22.8\r'
                            b'coincid=3250\r'
                            b'cs_diff=25.0\r'
                            b'prsslop=120\r'
                            b'prsoffs=70\r'
                            b'anlgcal=126\r\r'
                        )
                    elif line == b'mfginfo':
                        self.writer.write(
                            b"ser_num=28    \r"
                            b"mfgyear=15 \r"
                            b"mfg_mon=7  \r"
                            b"mfg_day=16 \r"
                            b"firmwar=4.1 \r\r"
                        )
                    elif line == b'read':
                        self._report()
                    elif line == b'raw=2':
                        self.writer.write((
                            f"smp_raw={self.data_RAWQsample:.0f}\r"
                            f"sat_raw={self.data_RAWQsaturator:.0f}\r"
                            f"prs_raw={self.data_RAWP:.0f}\r\r"
                        ).encode('ascii'))
                    elif line == b'rtclck':
                        ts = time.gmtime()
                        self.writer.write((
                            f"clkhour={ts.tm_hour}\r"
                            f"clk_min={ts.tm_min}\r"
                            f"clk_sec={ts.tm_sec}\r"
                            f"clkyear={ts.tm_year % 100}\r"
                            f"clk_mon={ts.tm_mon}\r"
                            f"clk_day={ts.tm_mday}\r"
                        ).encode('ascii'))
                    elif line == b'store':
                        pass
                    elif line.startswith(b'clkhour='):
                        pass
                    elif line.startswith(b'clk_min='):
                        pass
                    elif line.startswith(b'clk_sec='):
                        pass
                    elif line.startswith(b'clkyear='):
                        pass
                    elif line.startswith(b'clk_mon='):
                        pass
                    elif line.startswith(b'clk_day='):
                        pass
                    else:
                        raise ValueError
                except (ValueError, IndexError):
                    self.writer.write(b'ERR\r')
                await self.writer.drain()
        finally:
            await self._stop_unpolled()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
