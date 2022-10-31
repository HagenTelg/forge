import typing
import asyncio
import time
from enum import Enum, auto
from forge.dewpoint import extrapolate_rh
from forge.acquisition.instrument.streaming import StreamingSimulator
from forge.acquisition.instrument.tsi3563nephelometer.parameters import Parameters


def _format_exponent(value: float, decimals: int = 3, exponent: int = 1) -> str:
    raw = ('%.' + ('%d' % decimals) + 'e') % value
    (before, after) = raw.split('e')
    evalue = int(after[1:])
    return before + 'e' + after[:1] + ('%d' % evalue).rjust(exponent, '0')


class Simulator(StreamingSimulator):
    class _Mode(Enum):
        SAMPLE = auto()
        BLANK_ZERO_START = auto()
        BLANK_ZERO_END = auto()
        ZERO = auto()

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)

        self.unpolled_interval = 1.0
        self.unpolled_T = True
        self.unpolled_D = True
        self.unpolled_Y = True
        self.unpolled_B = True
        self.unpolled_G = True
        self.unpolled_R = True
        self._unpolled_task: typing.Optional[asyncio.Task] = None

        self.parameters = Parameters(
            SL="Some Cal",
            SMB=True,
            SMZ=4,
            SP=75,
            STA=1,
            STB=62,
            STP=32766,
            STZ=300,
            SVB=700,
            SVG=700,
            SVR=700,
            B=1,
            H=True,
            SKB=Parameters.SK(20000, 4.0e-3, 2.789E-5, 0.500),
            SKG=Parameters.SK(20000, 3.0e-3, 1.226E-5, 0.525),
            SKR=Parameters.SK(20000, 2.0e-3, 4.605E-6, 0.475),
        )

        self.total_revs = 10
        self.back_revs = 11

        self.data_Bs = [30.0, 20.0, 10.0]
        self.data_Bbs = [25.0, 15.0, 5.0]

        self.data_Cfx = [20000, 21000, 22000]
        self.data_Csx = [3000, 2000, 1000]
        self.data_Cbsx = [2900, 1900, 900]
        self.data_Cdx = [100, 200, 300]
        self.data_Cbdx = [90, 190, 290]

        self.data_Bsw = [31.0, 21.0, 11.0]
        self.data_Bbsw = [26.0, 16.0, 6.0]

        self.data_Tsample = 20.85
        self.data_Usample = 30.0
        self.data_Psample = 880.0
        self.data_Tinlet = 15.85
        self.data_Vl = 12.0
        self.data_Al = 6.0
        self.data_Vx = 0

        self.mode = self._Mode.SAMPLE
        self._zero_task: typing.Optional[asyncio.Task] = None
        self.remaining_time = 32000

        self.flags = 0

    def _correct_counts(self, c: float, wavelength: int) -> float:
        c *= c * self.parameters.K1(wavelength) * 1E-12 + 1.0
        return c

    @staticmethod
    def _frequency_total(c: float, width: float, revs: float) -> float:
        return (360.0 * c * 22.994) / (width * revs)

    def _output_counts(self, raw: float, width: float, revs: float, wavelength: int) -> float:
        return self._correct_counts(self._frequency_total(raw, width, revs), wavelength)

    @property
    def data_Uinlet(self) -> float:
        return extrapolate_rh(self.data_Tsample, self.data_Usample, self.data_Tinlet)

    @property
    def data_Cf(self) -> typing.List[float]:
        return [self._output_counts(self.data_Cfx[i], 40.0, self.total_revs, i) for i in range(3)]

    @property
    def data_Cs(self) -> typing.List[float]:
        return [self._output_counts(self.data_Csx[i], 140.0, self.total_revs, i) for i in range(3)]

    @property
    def data_Cbs(self) -> typing.List[float]:
        return [self._output_counts(self.data_Cbsx[i], 140.0, self.back_revs, i) for i in range(3)]

    @property
    def data_Cd(self) -> typing.List[float]:
        return [self._output_counts(self.data_Cdx[i], 60.0, self.total_revs, i) for i in range(3)]

    @property
    def data_Cbd(self) -> typing.List[float]:
        return [self._output_counts(self.data_Cbdx[i], 60.0, self.back_revs, i) for i in range(3)]

    def _record_T(self) -> None:
        ts = time.gmtime()
        self.writer.write((
            "T,"
            f"{ts.tm_year:04},"
            f"{ts.tm_mon:02},"
            f"{ts.tm_mday:02},"
            f"{ts.tm_hour:02},"
            f"{ts.tm_min:02},"
            f"{ts.tm_sec:02}"
            "\r"
        ).encode('ascii'))

    @property
    def data_modestring(self) -> str:
        mode = ""
        if self.mode == self._Mode.ZERO:
            mode = mode + "Z"
        elif self.mode == self._Mode.BLANK_ZERO_START or self.mode == self._Mode.BLANK_ZERO_END:
            mode = mode + "B"
        else:
            mode = mode + "N"
        if self.parameters.SMB:
            mode = mode + "B"
        else:
            mode = mode + "T"
        mode = mode + "XX"
        return mode

    def _record_D(self) -> None:
        self.writer.write((
            "D,"
            f"{self.data_modestring},"
            f"{self.remaining_time if self.parameters.SMZ or self.mode != self._Mode.SAMPLE else 0},"
            f"{','.join([_format_exponent(v * 1E-6) for v in self.data_Bs])},"
            f"{','.join([_format_exponent(v * 1E-6) for v in self.data_Bbs])}"
            "\r"
        ).encode('ascii'))

    def _record_Y(self) -> None:
        self.writer.write((
            "Y,"
            f"{round(self.data_Cf[1])},"
            f"{self.data_Psample:.1f},"
            f"{self.data_Tsample + 273.15:.1f},"
            f"{self.data_Tinlet + 273.15:.1f},"
            f"{self.data_Usample:.1f},"
            f"{self.data_Vl:.1f},"
            f"{self.data_Al:.1f},"
            f"{self.data_Vx},"
            f"{self.flags:04X}"
            "\r"
        ).encode('ascii'))

    def _record_BGR(self, wavelength: int) -> None:
        code = (["B", "G", "R"])[wavelength]
        self.writer.write((
            f"{code},"
            f"{self.data_Cfx[wavelength]},"
            f"{self.data_Csx[wavelength]},"
            f"{self.data_Cdx[wavelength]},"
            f"{self.total_revs},"
            f"{self.data_Cfx[wavelength]},"
            f"{self.data_Cbsx[wavelength]},"
            f"{self.data_Cbdx[wavelength]},"
            f"{self.back_revs},"
            f"{self.data_Psample:.1f},"
            f"{self.data_Tsample + 273.15:.1f}"
            "\r"
        ).encode('ascii'))

    def _record_Z(self) -> None:
        stp_factor = (self.data_Psample / 1013.25) * (273.15 / (self.data_Tsample + 273.15))
        Bsr = [
            self.parameters.SKB.K3 * stp_factor,
            self.parameters.SKG.K3 * stp_factor,
            self.parameters.SKR.K3 * stp_factor,
        ]

        self.writer.write(b"Z")
        for i in range(3):
            self.writer.write(b",")
            self.writer.write(_format_exponent(self.data_Bsw[i] * 1E-6 + Bsr[i]).encode('ascii'))
        for i in range(3):
            self.writer.write(b",")
            self.writer.write(_format_exponent(self.data_Bbsw[i] * 1E-6 + Bsr[i] * 0.5).encode('ascii'))
        for i in range(3):
            self.writer.write(b",")
            self.writer.write(_format_exponent(Bsr[i]).encode('ascii'))
        self.writer.write(b"\r")

    async def _unpolled(self) -> typing.NoReturn:
        while True:
            if self.unpolled_T:
                self._record_T()
            if self.unpolled_B:
                self._record_BGR(0)
            if self.unpolled_G:
                self._record_BGR(1)
            if self.unpolled_R:
                self._record_BGR(2)
            if self.unpolled_D:
                self._record_D()
            if self.unpolled_Y:
                self._record_Y()

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

    async def _zero(self) -> None:
        self.mode = self._Mode.BLANK_ZERO_START
        await asyncio.sleep(self.parameters.STB)
        self.mode = self._Mode.ZERO
        await asyncio.sleep(self.parameters.STZ)
        self.mode = self._Mode.BLANK_ZERO_END
        if self._unpolled_task:
            self._record_Z()
        await asyncio.sleep(self.parameters.STB)
        self.mode = self._Mode.SAMPLE

    async def _start_unpolled(self) -> None:
        await self._stop_unpolled()
        self._unpolled_task = asyncio.ensure_future(self._unpolled())

    async def _stop_zero(self) -> None:
        t = self._zero_task
        self._zero_task = None
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

    async def _start_zero(self) -> None:
        await self._stop_zero()
        self._zero_task = asyncio.ensure_future(self._zero())

    @property
    def is_in_zero(self) -> bool:
        return self.mode != self._Mode.SAMPLE

    async def run(self) -> typing.NoReturn:
        try:
            await self._start_unpolled()
            while True:
                line = await self.reader.readuntil(b'\r')
                line = line.strip()

                try:
                    if line == b'UE':
                        await self._stop_unpolled()
                        await self.writer.drain()
                        self.writer.write(b'OK\r')
                        await self.writer.drain()
                        continue
                    elif line == b'PU':
                        self.writer.write(b'OK\r')
                        await self.writer.drain()
                        continue

                    if self._unpolled_task:
                        # All other commands ignored while unpolled is active
                        continue

                    if line == b'UB':
                        self.writer.write(b'OK\r')
                        await self._start_unpolled()
                    elif line == b'SB0,0':
                        self.writer.write(b'OK\r')
                    elif line == b'SD0':
                        self.writer.write(b'OK\r')
                    elif line == b'SKB':
                        self.writer.write(Parameters.SK.write(self.parameters.SKB) + b'\r')
                    elif line.startswith(b'SKB'):
                        self.parameters.SKB = Parameters.SK.read(line[3:])
                        self.writer.write(b'OK\r')
                    elif line == b'SKG':
                        self.writer.write(Parameters.SK.write(self.parameters.SKG) + b'\r')
                    elif line.startswith(b'SKG'):
                        self.parameters.SKG = Parameters.SK.read(line[3:])
                        self.writer.write(b'OK\r')
                    elif line == b'SKR':
                        self.writer.write(Parameters.SK.write(self.parameters.SKR) + b'\r')
                    elif line.startswith(b'SKR'):
                        self.parameters.SKR = Parameters.SK.read(line[3:])
                        self.writer.write(b'OK\r')
                    elif line == b'SL':
                        self.writer.write(self.parameters.SL.encode('utf-8') + b'\r')
                    elif line.startswith(b'SL'):
                        self.parameters.SL = line[2:].decode('utf-8')
                        self.writer.write(b'OK\r')
                    elif line == b'SMB':
                        self.writer.write(b'%d\r' % (1 if self.parameters.SMB else 0))
                    elif line.startswith(b'SMB'):
                        self.parameters.SMB = bool(int(line[3:]))
                        self.writer.write(b'OK\r')
                    elif line == b'SMZ':
                        self.writer.write(b'%d\r' % self.parameters.SMZ)
                    elif line.startswith(b'SMZ'):
                        self.parameters.SMZ = int(line[3:])
                        self.writer.write(b'OK\r')
                    elif line == b'SP':
                        self.writer.write(b'%d\r' % self.parameters.SP)
                    elif line.startswith(b'SP'):
                        self.parameters.SP = int(line[2:])
                        self.writer.write(b'OK\r')
                    elif line == b'STA':
                        self.writer.write(b'%d\r' % self.parameters.STA)
                    elif line.startswith(b'STA'):
                        self.parameters.STA = int(line[3:])
                        self.writer.write(b'OK\r')
                    elif line == b'STB':
                        self.writer.write(b'%d\r' % self.parameters.STB)
                    elif line.startswith(b'STB'):
                        self.parameters.STB = int(line[3:])
                        self.writer.write(b'OK\r')
                    elif line == b'STP':
                        self.writer.write(b'%d\r' % self.parameters.STP)
                    elif line.startswith(b'STP'):
                        self.parameters.STP = int(line[3:])
                        self.writer.write(b'OK\r')
                    elif line.startswith(b'STT'):
                        self.writer.write(b'OK\r')
                    elif line == b'STZ':
                        self.writer.write(b'%d\r' % self.parameters.STZ)
                    elif line.startswith(b'STZ'):
                        self.parameters.STZ = int(line[3:])
                        self.writer.write(b'OK\r')
                    elif line == b'SVB':
                        self.writer.write(b'%d\r' % self.parameters.SVB)
                    elif line.startswith(b'SVB'):
                        self.parameters.SVB = int(line[3:])
                        self.writer.write(b'OK\r')
                    elif line == b'SVG':
                        self.writer.write(b'%d\r' % self.parameters.SVG)
                    elif line.startswith(b'SVG'):
                        self.parameters.SVG = int(line[3:])
                        self.writer.write(b'OK\r')
                    elif line == b'SVR':
                        self.writer.write(b'%d\r' % self.parameters.SVR)
                    elif line.startswith(b'SVR'):
                        self.parameters.SVR = int(line[3:])
                        self.writer.write(b'OK\r')
                    elif line == b'SX':
                        self.writer.write(b'%d\r' % self.data_Vx)
                    elif line.startswith(b'SX'):
                        self.data_Vx = int(line[2:])
                        self.writer.write(b'OK\r')
                    elif line == b'B':
                        self.writer.write(b'%d\r' % self.parameters.B)
                    elif line.startswith(b'B'):
                        self.parameters.B = int(line[1:])
                        self.writer.write(b'OK\r')
                    elif line == b'H':
                        self.writer.write(b'%d\r' % (1 if self.parameters.H else 0))
                    elif line.startswith(b'H'):
                        self.parameters.H = bool(int(line[1:]))
                        self.writer.write(b'OK\r')
                    elif line == b'Z':
                        if self.mode != self._Mode.SAMPLE:
                            raise ValueError
                        self.mode = self._Mode.BLANK_ZERO_START
                        await self._start_zero()
                        self.writer.write(b'OK\r')
                    elif line == b'RD':
                        self._record_D()
                    elif line == b'RY':
                        self._record_Y()
                    elif line == b'RP':
                        for i in range(3):
                            self._record_BGR(i)
                    elif line == b'RPG':
                        self._record_BGR(1)
                    elif line == b'RT':
                        self._record_T()
                    elif line == b'RU':
                        if self.unpolled_T:
                            self._record_T()
                        if self.unpolled_B:
                            self._record_BGR(0)
                        if self.unpolled_G:
                            self._record_BGR(1)
                        if self.unpolled_R:
                            self._record_BGR(2)
                        if self.unpolled_D:
                            self._record_D()
                        if self.unpolled_Y:
                            self._record_Y()
                    elif line == b'RV':
                        self.writer.write(b"Revision E, May 1996\r")
                    elif line == b'RY':
                        self._record_Y()
                    elif line == b'RZ':
                        self._record_Z()
                    elif line.startswith(b'UD'):
                        self.unpolled_D = bool(int(line[2:]))
                        self.writer.write(b'OK\r')
                    elif line.startswith(b'UP'):
                        enable = bool(int(line[2:]))
                        self.unpolled_B = enable
                        self.unpolled_G = enable
                        self.unpolled_R = enable
                        self.writer.write(b'OK\r')
                    elif line.startswith(b'UT'):
                        self.unpolled_T = bool(int(line[2:]))
                        self.writer.write(b'OK\r')
                    elif line.startswith(b'UY'):
                        self.unpolled_T = bool(int(line[2:]))
                        self.writer.write(b'OK\r')
                    elif line == b'UZ1':
                        self.writer.write(b'OK\r')
                    elif line == b'VN':
                        self.writer.write(b'OK\r')
                    elif line == b'VZ':
                        self.writer.write(b'OK\r')
                    else:
                        raise ValueError
                except (ValueError, IndexError):
                    self.writer.write(b'ERROR\r')
                await self.writer.drain()
        finally:
            await self._stop_unpolled()
            await self._stop_zero()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
