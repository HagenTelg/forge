import typing
import asyncio
import enum
from math import nan
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    class MajorState(enum.IntEnum):
        Normal = 0
        SpanCalibration = 1
        ZeroCalibration = 2
        SpanCheck = 3
        ZeroCheck = 4
        ZeroAdjust = 5
        SystemCalibration = 6
        EnvironmentalCalibration = 7

    class NormalMinorState(enum.IntEnum):
        ShutterDown = 0
        ShutterMeasure = 1
        ShutterUp = 2
        Measure = 3

    class FilterMode(enum.Enum):
        Disable = "None"
        Kalman = "Kalman"
        MovingAverage = "Moving Average"

    class DigitalState(enum.IntFlag):
        CellHeaterOff = (1 << 0)
        InletHeaterOff = (1 << 1)
        SamplePumpOn = (1 << 2)
        ZeroPumpOn = (1 << 3)
        SpanGasValveOpen = (1 << 4)
        Auxiliary = (1 << 7)

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, polar_mode: bool = False):
        super().__init__(reader, writer)

        self.major_state: Simulator.MajorState = self.MajorState.Normal
        self.minor_state: typing.Union[Simulator.NormalMinorState] = self.NormalMinorState.Measure

        self.data_Tsample = 20.85
        self.data_Usample = 30.0
        self.data_Psample = 880.0
        self.data_Tcell = 15.85

        self.data_Cd = 20.0

        self.data_Bn = [0.0, 90.0]
        self.data_Bsn = [
            [30.0, 20.0, 10.0],
            [12.0, 6.0, 4.0],
        ]

        self.data_Cf = [200000.0, 150000.0, 100000.0]
        self.data_Cs = [20000.0, 15000.0, 10000.0]
        self.data_Cbs = [12000.0, 6000.0, 4000.0]
        self.data_Cr = [0.02, 0.015, 0.01]
        self.data_Cbr = [0.012, 0.006, 0.004]

        self.parameter_ZeroAdjX = [
            [100.0, 90.0, 60.0],
            [50.0, 45.0, 30.0],
        ]
        self.parameter_ZeroAdjY = [
            [0.007, 0.008, 0.003],
            [0.002, 0.009, 0.004],
        ]
        self.parameter_calM = [
            [0.000060, 0.000080, 0.000100],
            [0.000070, 0.000090, 0.000110],
        ]
        self.parameter_calC = [
            [0.00060, 0.00080, 0.00100],
            [0.00070, 0.00090, 0.00110],
        ]
        self.parameter_calWall = [
            [70.0, 80.0, 90.0],
            [71.0, 81.0, 91.0],
        ]

        self.flags = 0
        self.digital_state = self.DigitalState(0)
        self.filter_mode = self.FilterMode.Kalman
        self.standard_temperature: typing.Optional[float] = 0.0

        self.enable_backscatter = True
        self.enable_filter_control = True
        self.instrument_id = "Ecotech Aurora 3000 Nephelometer v1.20.000, ID #111444"
        self.garble_inject: typing.Optional[bytes] = None

        self.blocked_busy: bool = False
        self._zero_task: typing.Optional[asyncio.Task] = None
        self._shutter_task: typing.Optional[asyncio.Task] = None

        if polar_mode:
            self.make_polar()

    def make_polar(self, angles: int = 18):
        self.instrument_id = "Ecotech Aurora 4000 Nephelometer v1.20.000, ID #111444"

        self.data_Bn.clear()
        self.data_Bn.append(0.0)
        for angle in range(1, angles):
            self.data_Bn.append(5.0 + angle * 5.0)

        self.data_Bsn.clear()
        self.parameter_ZeroAdjX.clear()
        self.parameter_ZeroAdjY.clear()
        self.parameter_calM.clear()
        self.parameter_calC.clear()
        self.parameter_calWall.clear()
        for angle in range(18):
            div = angle + 1
            self.data_Bsn.append([30.0 / div, 20.0 / div, 10.0 / div])
            self.parameter_ZeroAdjX.append([100.0 / div, 90.0 / div, 60.0 / div])
            self.parameter_ZeroAdjY.append([0.005 / div, 0.004 / div, 0.03 / div])
            self.parameter_calM.append([0.000060 / div, 0.000080 / div, 0.000100 / div])
            self.parameter_calC.append([0.00060 / div, 0.00080 / div, 0.00100 / div])
            self.parameter_calWall.append([70.0, 80.0, 90.0])

    def angle_index(self, angle: float, max_deviation: typing.Optional[float] = 5.0) -> typing.Optional[int]:
        best = None
        for i in range(len(self.data_Bn)):
            check = self.data_Bn[i]
            deviation = abs(check - angle)
            if max_deviation is not None and deviation > max_deviation:
                continue
            if best is not None and deviation >= abs(angle - self.data_Bn[best]):
                continue
            best = i
        return best

    @property
    def data_Bs(self) -> typing.List[float]:
        i = self.angle_index(0)
        if i is None:
            return [nan, nan, nan]
        return self.data_Bsn[i]

    @property
    def data_Bbs(self) -> typing.List[float]:
        i = self.angle_index(90)
        if i is None:
            return [nan, nan, nan]
        return self.data_Bsn[i]

    @property
    def data_Bswn(self) -> typing.List[typing.List[float]]:
        result = list()
        for angle in range(len(self.data_Bn)):
            calC = self.parameter_calC[angle]
            calM = self.parameter_calM[angle]
            result.append([calC[i] / calM[i] for i in range(3)])
        return result

    @property
    def data_Bsw(self) -> typing.List[float]:
        i = self.angle_index(0)
        if i is None:
            return [nan, nan, nan]
        return self.data_Bswn[i]

    @property
    def data_Bbsw(self) -> typing.List[float]:
        i = self.angle_index(0)
        if i is None:
            return [nan, nan, nan]
        return self.data_Bswn[i]

    @property
    def is_in_zero(self):
        return self.DigitalState.ZeroPumpOn in self.digital_state

    async def _zero(self) -> None:
        try:
            self.major_state = self.MajorState.ZeroAdjust
            self.digital_state |= self.DigitalState.ZeroPumpOn
            self.digital_state &= ~self.DigitalState.SamplePumpOn
            await asyncio.sleep(1200)
            self.blocked_busy = True
            await asyncio.sleep(60)
        finally:
            self.major_state = self.MajorState.Normal
            self.digital_state |= self.DigitalState.SamplePumpOn
            self.digital_state &= ~self.DigitalState.ZeroPumpOn
            self.blocked_busy = False

    async def _start_zero(self) -> None:
        await self._stop_zero()
        self._zero_task = asyncio.ensure_future(self._zero())

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

    async def _shutter(self) -> typing.NoReturn:
        shutter_interval = max(30.0, len(self.data_Bn) * 3.5)
        while True:
            self.minor_state = self.NormalMinorState.Measure
            await asyncio.sleep(shutter_interval)
            self.minor_state = self.NormalMinorState.ShutterDown
            await asyncio.sleep(1.0)
            self.minor_state = self.NormalMinorState.ShutterMeasure
            await asyncio.sleep(3.0)
            self.minor_state = self.NormalMinorState.ShutterUp
            await asyncio.sleep(1.0)

    async def _start_shutter(self) -> None:
        await self._stop_shutter()
        self._shutter_task = asyncio.ensure_future(self._shutter())

    async def _stop_shutter(self) -> None:
        t = self._shutter_task
        self._shutter_task = None
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

    def _ee(self) -> None:
        self.writer.write(b"Temperature Unit    =, \xC2\xB0\x43\r\n")
        self.writer.write(b"AtmPressureUnit     =, mb\r\n")
        if self.standard_temperature is not None:
            self.writer.write(b"Normalise to        =, %.0f\xC2\xB0\x43\r\n" % self.standard_temperature)
        else:
            self.writer.write(b"Normalise to        =, \r\n")
        self.writer.write(b"Filtering Method    =, %s\r\n" % (str(self.filter_mode.value).encode('ascii')))
        self.writer.write(b"Wavelength 1        =, 635nm \r\n")
        self.writer.write(b"Wavelength 2        =, 525nm \r\n")
        self.writer.write(b"Wavelength 3        =, 450nm \r\n")
        self.writer.write(b"Angle Count         =, %d\r\n" % len(self.data_Bn))
        self.writer.write(b"Angle List          =, ")
        self.writer.write(b",".join([b"%.0f" % v for v in self.data_Bn]))
        self.writer.write(b"\r\n")

        def angle_array(prefix: str, values: typing.List[typing.List[float]], fmt: str = "%.3f"):
            for wavelength in range(3):
                self.writer.write((prefix + str(wavelength+1) + " =,  ").encode('utf-8'))
                self.writer.write((", ".join([
                    fmt % values[angle][2-wavelength] for angle in range(len(values))
                ])).encode('utf-8'))
                self.writer.write(b"\r\n")

        angle_array("Calibration Ms    ", self.parameter_calM, "%.6f")
        angle_array("Calibration Cs    ", self.parameter_calC, "%.6f")
        angle_array("Calibration Walls ", self.parameter_calWall)
        angle_array("Cal ZeroAdj Xs    ", self.parameter_ZeroAdjX)
        angle_array("Cal ZeroAdj Ys    ", self.parameter_ZeroAdjY, "%.6f")

        self.writer.write(b"Cal ZeroAdj Temp    =,  298.789\r\n")
        self.writer.write(b"Cal ZeroAdj Pressure=,  823.359\r\n")

    def _data_line(self) -> None:
        self.writer.write(",".join([
            "21/11/2003 09:56:10",
            *[f"{v:.3f}" for angle in self.data_Bsn for v in reversed(angle)],
            f"{self.data_Tsample:.3f}",
            f"{self.data_Tcell:.3f}",
            f"{self.data_Usample:.3f}",
            f"{self.data_Psample:.3f}",
            f"{self.major_state.value:02d}",
            f"{self.digital_state:02X}",
        ]).encode('ascii'))
        self.writer.write(b"\r\n")

    async def run(self) -> typing.NoReturn:
        try:
            await self._start_shutter()
            while True:
                line = await self.reader.readuntil(b'\r')
                line = line.strip()

                if self.blocked_busy:
                    self.writer.write(b'\x15')
                    continue

                if self.garble_inject:
                    self.writer.write(self.garble_inject + b"\r\n")
                    self.garble_inject = None
                    continue

                try:
                    if line == b'ID0':
                        self.writer.write(self.instrument_id.encode('ascii') + b"\r")
                    elif line == b'EE':
                        self._ee()
                    elif line == b'**0J5':
                        await self._start_zero()
                        self.writer.write(b"OK\r\n")
                    elif line == b'**0B':
                        pass
                    elif line.startswith(b'DO000') and len(line) > 5:
                        do_span = line[5:6] == b'1'
                        if do_span:
                            self.digital_state |= self.DigitalState.SpanGasValveOpen
                            self.digital_state &= ~(self.DigitalState.SamplePumpOn | self.DigitalState.ZeroPumpOn)
                        else:
                            self.digital_state |= self.DigitalState.ZeroPumpOn
                            self.digital_state &= ~(self.DigitalState.SpanGasValveOpen | self.DigitalState.ZeroPumpOn)
                        self.writer.write(b"OK\r\n")
                    elif line.startswith(b'DO001') and len(line) > 5:
                        do_zero = line[5:6] == b'1'
                        if do_zero:
                            self.digital_state |= self.DigitalState.ZeroPumpOn
                            self.digital_state &= ~self.DigitalState.SamplePumpOn
                        else:
                            self.digital_state |= self.DigitalState.SamplePumpOn
                            self.digital_state &= ~self.DigitalState.ZeroPumpOn
                        self.writer.write(b"OK\r\n")
                    elif line.startswith(b'**0PCF,') and len(line) > 7 and self.enable_filter_control:
                        mode = line[7:8]
                        if mode == b'K':
                            self.filter_mode = self.FilterMode.Kalman
                        elif mode == b'N':
                            self.filter_mode = self.FilterMode.Disable
                        elif mode == b'M':
                            self.filter_mode = self.FilterMode.MovingAverage
                        else:
                            raise ValueError
                        self.writer.write(b"OK\r\n")
                    elif line.startswith(b'**0PCSTP,') and len(line) > 9:
                        if line[9:10] == b'N':
                            self.standard_temperature = None
                        else:
                            self.standard_temperature = float(line[9:])
                        self.writer.write(b"OK\r\n")
                    elif line.startswith(b'VI') and len(line) >= 5:
                        n = int(line[2:])

                        def _response(data: typing.Union[float, int]) -> None:
                            data = b"%09.5f" % float(data)
                            self.writer.write(data + b'\r\n')

                        if n == 0:
                            self.writer.write(f"{self.major_state.value:02d}.{self.minor_state.value:02d}\r\n".encode('ascii'))
                        elif n == 88:
                            self.writer.write(b" %d\r\n" % self.flags)
                        elif n == 99:
                            self._data_line()
                        elif n == 4:
                            _response(self.data_Cd)
                        elif n == 6:
                            _response(self.data_Cf[2])
                        elif n == 7:
                            _response(self.data_Cs[2])
                        elif n == 8:
                            _response(self.data_Cr[2])
                        elif n == 33 and self.enable_backscatter:
                            _response(self.data_Cbs[2])
                        elif n == 34 and self.enable_backscatter:
                            _response(self.data_Cbr[2])
                        elif n == 9:
                            _response(self.data_Cf[1])
                        elif n == 10:
                            _response(self.data_Cs[1])
                        elif n == 11:
                            _response(self.data_Cr[1])
                        elif n == 35 and self.enable_backscatter:
                            _response(self.data_Cbs[1])
                        elif n == 36 and self.enable_backscatter:
                            _response(self.data_Cbr[1])
                        elif n == 12:
                            _response(self.data_Cf[0])
                        elif n == 13:
                            _response(self.data_Cs[0])
                        elif n == 14:
                            _response(self.data_Cr[0])
                        elif n == 37 and self.enable_backscatter:
                            _response(self.data_Cbs[0])
                        elif n == 38 and self.enable_backscatter:
                            _response(self.data_Cbr[0])
                        else:
                            raise ValueError
                    elif line.startswith(b'**0S') or line.startswith(b'**0PC') or line.startswith(b'DO0'):
                        self.writer.write(b"OK\r\n")
                    else:
                        raise ValueError
                except (ValueError, IndexError):
                    self.writer.write(b'ERROR\r\n')
        finally:
            await self._stop_zero()
            await self._stop_shutter()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import arguments, parse_arguments, run

    parser = arguments()
    parser.add_argument('--polar',
                        dest='polar', action='store_true',
                        help="enable enable polar mode")

    args, _ = parser.parse_known_args()

    run(parse_arguments(), Simulator, polar_mode=args.polar)
