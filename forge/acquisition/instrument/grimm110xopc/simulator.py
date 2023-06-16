import typing
import asyncio
import time
from forge.units import flow_lpm_to_ccs, flow_lpm_to_m3s
from forge.acquisition.instrument.streaming import StreamingSimulator


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, enable_mass: bool = False):
        super().__init__(reader, writer)

        self._unpolled_task: typing.Optional[asyncio.Task] = None
        self._unpolled_volume_valid: bool = True
        self.mass_concentrations: bool = False

        self.model_number = "1.109"
        self.firmware_version = "12.30 E"
        self.flow_correction = 1.2
        self.gf = 1.0
        self.gf_scale = 20.0

        self.data_Q = 1.2
        self.data_PCTpump = 40.0
        self.data_PCTbattery = 70.0
        self.data_X1 = 10.0
        self.data_X25 = 15.0
        self.data_X10 = 20.0

        self._volume_begin_time: float = time.monotonic()

        self.data_Dlower = [
            0.25, 0.28, 0.30, 0.35, 0.40, 0.45, 0.50, 0.58,
            0.65, 0.70, 0.80, 1.0, 1.3, 1.6, 2.0, 2.5,
            3.0, 3.5, 4.0, 5.0, 6.5, 7.5, 8.5, 10.0,
            12.5, 15.0, 17.5, 20.0, 25.0, 30.0, 32.0,
        ]
        self.data_Csum = [0] * len(self.data_Dlower)
        for i in reversed(range(len(self.data_Csum))):
            self.data_Csum[i] += 100 + i * 5
            if i+1 < len(self.data_Csum):
                self.data_Csum[i] += self.data_Csum[i+1]

        if enable_mass:
            self.mass_concentrations = True
            self.model_number = "180MC"
            self.firmware_version = "7.80 US"

        self.flags = 0

    @property
    def data_X(self) -> typing.List[float]:
        return [self.data_X1, self.data_X25, self.data_X10]

    @property
    def data_Dp(self) -> typing.List[float]:
        return [
            (self.data_Dlower[i] + self.data_Dlower[i+1]) / 2 if i+1 < len(self.data_Dlower)
            else self.data_Dlower[i] + (self.data_Dlower[i] + self.data_Dlower[i-1]) / 2
            for i in range(len(self.data_Dlower))
        ]

    @property
    def data_Cb(self) -> typing.List[float]:
        return [
            self.data_Csum[i] - self.data_Csum[i+1] if i+1 < len(self.data_Csum) else self.data_Csum[i]
            for i in range(len(self.data_Csum))
        ]

    @property
    def data_dN(self) -> typing.List[float]:
        factor = self.flow_correction / (flow_lpm_to_ccs(self.data_Q) * 6.0)
        return [c * factor for c in self.data_Cb]

    @property
    def data_N(self) -> float:
        return sum(self.data_dN)

    def _output_array(self, values: typing.List[float], identifier: bytes, decimals: int = 0) -> None:
        fmt = f"%8.{decimals}f".encode('ascii')

        def bins(values: typing.Iterable[float]) -> bytes:
            return b" ".join([(fmt % v) for v in values])

        if len(values) <= 8:
            self.writer.write(identifier.upper())
            self.writer.write(b": ")
            self.writer.write(bins(values))
            self.writer.write(b"\r")
        elif len(values) <= 15:
            self.writer.write(identifier.upper())
            self.writer.write(b": ")
            self.writer.write(bins(values[:8]))
            self.writer.write(b"\r")

            self.writer.write(identifier.lower())
            self.writer.write(b": ")
            self.writer.write(bins(values[7:]))
            self.writer.write(b"\r")
        else:
            self.writer.write(identifier.upper())
            self.writer.write(b": ")
            self.writer.write(bins(values[:8]))
            self.writer.write(b"\r")
            self.writer.write(identifier.upper())
            self.writer.write(b"; ")
            self.writer.write(bins(values[8:16]))
            self.writer.write(b"\r")

            self.writer.write(identifier.lower())
            self.writer.write(b": ")
            self.writer.write(bins(values[15:23]))
            if self.mass_concentrations:
                self.writer.write(b"     0")
            self.writer.write(b"\r")
            self.writer.write(identifier.lower())
            self.writer.write(b"; ")
            self.writer.write(bins(values[23:]))
            if self.mass_concentrations:
                self.writer.write(b"     0")
            self.writer.write(b"\r")

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

    async def _unpolled(self) -> typing.NoReturn:
        minute_index = 0
        self._unpolled_volume_valid = False
        while True:
            # Ignore volume commands right around when a record is output
            await asyncio.sleep(1.0)
            self._unpolled_volume_valid = True
            await asyncio.sleep(4.0)
            self._unpolled_volume_valid = False
            await asyncio.sleep(1.0)

            if minute_index == 0:
                ts = time.gmtime()
                self.writer.write((
                                      "P: "
                                      f"{ts.tm_year % 100} {ts.tm_mon} {ts.tm_mday} {ts.tm_hour} {ts.tm_min} "
                                      "0 "
                                      f"{self.gf * self.gf_scale:.0f} "
                                      f"{self.flags} "
                                      f"{self.data_PCTbattery:.0f} "
                                      f"{self.data_PCTpump:.0f} "
                                      "0 0 0 0 0\r"
                                  ).encode('ascii'))
                self.writer.write(b"K: 0 0 0 0\r")

            if self.mass_concentrations:
                self.writer.write((
                                      f"N{minute_index}, "
                                      f"{self.data_X10 * 10.0:4.0f} "
                                      f"{self.data_X25 * 10.0:4.0f} "
                                      f"{self.data_X1 * 10.0:4.0f}\r"
                                  ).encode('ascii'))

            self._output_array(self.data_Csum, b"C%d" % minute_index)
            self._unpolled_output_time = time.monotonic()

            await self.writer.drain()
            minute_index = (minute_index + 1) % 10

    async def run(self) -> typing.NoReturn:
        try:
            await self._start_unpolled()
            while True:
                line = await self.reader.readuntil(b'\r')
                line = line.strip()

                # Echo
                self.writer.write(line)
                self.writer.write(b'\r')

                try:
                    if line == b"C":
                        pass
                    elif line == b"F":
                        pass
                    elif line == b"J":
                        if self.mass_concentrations:
                            self.writer.write(b"J:  PM10 PM2.5 PM1.0\r")
                            self._output_array(self.data_Dlower, b"J ", 2)
                        else:
                            self._output_array(self.data_Dlower, b"Jc", 2)
                    elif line == b"M":
                        # Ignore M commands right after an unpolled response
                        if not self._unpolled_task or self._unpolled_volume_valid:
                            if self.mass_concentrations:
                                self.writer.write(b"Mean PM10:     0.0 ; PM2.5:     0.0 ; PM1:     0.0\r")
                            else:
                                self._output_array(self.data_Cb, b"Mc")
                            elapsed = time.monotonic() - self._volume_begin_time
                            total_volume = flow_lpm_to_m3s(self.data_Q) * elapsed
                            self.writer.write(f"V: {total_volume:.6f} m3\r".encode('ascii'))
                    elif line == b"R":
                        await self._start_unpolled()
                    elif line == b"S":
                        await self._stop_unpolled()
                    elif line == b"V":
                        self.writer.write(f"Version: {self.firmware_version}\r".encode('ascii'))
                    elif line == b"!":
                        self.writer.write(f"Model {self.model_number}  Version: {self.firmware_version}\r".encode('ascii'))
                    elif line == b"@":
                        self.writer.write(b"Ser.No: 9G040001\r")
                    else:
                        raise ValueError
                except (ValueError, IndexError):
                    self.writer.write(b'ERROR\r')
                await self.writer.drain()
        finally:
            await self._stop_unpolled()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import arguments, parse_arguments, run

    parser = arguments()
    parser.add_argument('--mass',
                        dest='mass', action='store_true',
                        help="enable mass concentration output")

    args, _ = parser.parse_known_args()

    run(parse_arguments(), Simulator, enable_mass=args.mass)
