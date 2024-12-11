import typing
import asyncio
import logging
import struct
import time
from math import nan
from forge.acquisition.instrument.streaming import StreamingSimulator

_LOGGER = logging.getLogger(__name__)


class _ErrorResponse(ValueError):
    def __init__(self, error_code: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.error_code = error_code


class Simulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, polar_mode: bool = False):
        super().__init__(reader, writer)

        self.data_Tsample = 20.85
        self.data_Usample = 30.0
        self.data_Psample = 880.0
        self.data_Tchassis = 19.85
        self.data_Uchassis = 31.0
        self.data_Pchassis = 881.0
        self.data_Q = 5.0

        self.data_Cd = 20.0
        self.data_Cf = [10000, 20000, 30000]

        self.model_number = 300

        self.data_Bn = [0.0, 90.0]
        self.data_Bsn = [
            [30.0, 20.0, 10.0],
            [12.0, 6.0, 4.0],
        ]
        self.data_Csn = [
            [1000.0, 2000.0, 3000.0],
            [500.0, 600.0, 700.0],
        ]

        self.parameter_calM = [
            [0.000060, 0.000080, 0.000100],
            [0.000070, 0.000090, 0.000110],
        ]
        self.parameter_calC = [
            [0.00060, 0.00080, 0.00100],
            [0.00070, 0.00090, 0.00110],
        ]

        self.serial_id = 0

        self._current_operation: int = 0
        self._zero_task: typing.Optional[asyncio.Task] = None

        if polar_mode:
            self.make_polar()

    def make_polar(self, angles: int = 18) -> None:
        self.model_number = 400

        self.data_Bn.clear()
        self.data_Bn.append(0.0)
        for angle in range(1, angles):
            self.data_Bn.append(5.0 + angle * 5.0)

        self.data_Bsn.clear()
        self.data_Csn.clear()
        self.parameter_calM.clear()
        self.parameter_calC.clear()
        for angle in range(18):
            div = angle + 1
            self.data_Bsn.append([30.0 / div, 20.0 / div, 10.0 / div])
            self.data_Csn.append([3000.0 / div, 2000.0 / div, 1000.0 / div])
            self.parameter_calM.append([0.000060 / div, 0.000080 / div, 0.000100 / div])
            self.parameter_calC.append([0.00060 / div, 0.00080 / div, 0.00100 / div])

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
    def data_Cs(self) -> typing.List[float]:
        i = self.angle_index(0)
        if i is None:
            return [nan, nan, nan]
        return self.data_Csn[i]

    @property
    def data_Cbs(self) -> typing.List[float]:
        i = self.angle_index(90)
        if i is None:
            return [nan, nan, nan]
        return self.data_Csn[i]

    @property
    def data_Cr(self) -> typing.List[float]:
        i = self.angle_index(0)
        if i is None:
            return [nan, nan, nan]
        return [self.data_Csn[i][w] / self.data_Cf[w] for w in range(3)]

    @property
    def data_Cbr(self) -> typing.List[float]:
        i = self.angle_index(90)
        if i is None:
            return [nan, nan, nan]
        return [self.data_Csn[i][w] / self.data_Cf[w] for w in range(3)]

    @property
    def data_Crn(self) -> typing.List[typing.List[float]]:
        result = list()
        for angle in range(len(self.data_Bn)):
            result.append([self.data_Csn[angle][i] / self.data_Cf[i] for i in range(3)])
        return result

    async def _zero(self) -> None:
        self._current_operation = 1
        await asyncio.sleep(10)
        self._current_operation = 3

    async def start_zero(self) -> None:
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

    @property
    def is_in_zero(self) -> bool:
        return self._current_operation == 1

    @staticmethod
    def _checksum(data: bytes) -> int:
        r = 0
        for b in data:
            r ^= b
        return r

    async def _read_command(self) -> typing.Tuple[int, bytes]:
        while True:
            header = await self.reader.readexactly(6)
            stx, serial_id, command, etx, msg_len = struct.unpack('>BBBBH', header)
            if stx != 0x02:
                raise ValueError
            if etx != 0x03:
                raise ValueError
            if msg_len:
                data = await self.reader.readexactly(msg_len)
            else:
                data = bytes()
            footer = await self.reader.readexactly(2)
            received_checksum, eot = struct.unpack('>BB', footer)
            if eot != 0x04:
                raise ValueError
            calculated_checksum = self._checksum(header + data)
            if calculated_checksum != received_checksum:
                raise _ErrorResponse(0)

            if serial_id == 0 or serial_id == self.serial_id:
                return command, data

    async def _write_response(self, command: int, data: bytes = None) -> None:
        packet = struct.pack('>BBBBH', 0x02, self.serial_id, command, 0x03, len(data) if data else 0)
        if data:
            packet += data
        self.writer.write(packet + struct.pack('>BB', self._checksum(packet), 0x04))

    async def _write_values(self, *values, command: int = 4) -> None:
        data = bytearray()
        for v in values:
            if isinstance(v, int):
                data += struct.pack('>I', v)
            else:
                data += struct.pack('>f', v)
        await self._write_response(command, bytes(data))

    def _get_value(self, parameter: int) -> bytes:
        if parameter >= 1_000_000:
            measurement = parameter // 1_000_000
            parameter -= measurement * 1_000_000
            wavelength = parameter // 1_000
            parameter -= wavelength * 1_000
            angle = parameter

            if wavelength == 450:
                wavelength = 0
            elif wavelength == 525:
                wavelength = 1
            elif wavelength == 635:
                wavelength = 2
            else:
                raise _ErrorResponse(2)

            for idx in range(len(self.data_Bn)):
                if int(self.data_Bn[idx]) == angle:
                    angle = idx
                    break
            else:
                raise _ErrorResponse(2)

            if measurement in (1, 2, 3, 4, 5):
                return struct.pack('>f', self.data_Bsn[angle][wavelength])
            elif measurement in (6, 7, 8, 9, 10):
                return struct.pack('>f', self.data_Crn[angle][wavelength])
            elif measurement == 11:
                return struct.pack('>f', self.data_Csn[angle][wavelength])
            elif measurement == 13:
                return struct.pack('>f', self.data_Cd)
            elif measurement == 15:
                return struct.pack('>f', self.data_Cf[wavelength])
            elif measurement == 20:
                if self._current_operation == 3:
                    self._current_operation = 0
                return struct.pack('>f', self.parameter_calM[angle][wavelength])
            elif measurement == 21:
                if self._current_operation == 3:
                    self._current_operation = 0
                return struct.pack('>f', self.parameter_calC[angle][wavelength])
            elif measurement == 27:
                return struct.pack('>I', round(time.time()) ^ 0xFFFF_FFFF)
        elif parameter == 1:
            now = time.gmtime()
            bits = \
                (now.tm_sec & 0b111111) << 0 | \
                (now.tm_min & 0b111111) << 6 | \
                (now.tm_hour & 0b11111) << 12 | \
                (now.tm_mday & 0b11111) << 17 | \
                (now.tm_mon & 0b1111) << 22 | \
                ((now.tm_year - 2000) & 0b111111) << 26
            return struct.pack('>I', bits)
        elif parameter == 4001:
            # Number of wavelengths
            return struct.pack('>I', 3)
        elif parameter == 4002:
            # Number of angles
            return struct.pack('>I', len(self.data_Bn))
        elif 4004 <= parameter <= 4006:
            # Wavelength
            return struct.pack('>I', (450, 525, 635)[parameter - 4004])
        elif 4008 <= parameter <= 4027:
            return struct.pack('>I', int(round(self.data_Bn[parameter - 4008])))
        elif parameter == 4035:
            return struct.pack('>I', self._current_operation)
        elif parameter == 5001:
            return struct.pack('>f', self.data_Tsample)
        elif parameter == 5002:
            return struct.pack('>f', self.data_Psample)
        elif parameter == 5003:
            return struct.pack('>f', self.data_Usample)
        elif parameter == 5004:
            return struct.pack('>f', self.data_Tchassis)
        elif parameter == 5005:
            return struct.pack('>f', self.data_Pchassis)
        elif parameter == 5006:
            return struct.pack('>f', self.data_Uchassis)
        elif parameter == 5010:
            return struct.pack('>f', self.data_Q)
        raise _ErrorResponse(2)

    def _set_value(self, parameter: int, value: bytes) -> None:
        if parameter == 1:
            _LOGGER.debug("Set time: 0x%08X", struct.unpack('>I', value)[0])
            return
        raise _ErrorResponse(2)

    async def run(self) -> typing.NoReturn:
        try:
            while True:
                try:
                    command, data = await self._read_command()

                    if command == 1 and not data:
                        await self._write_values(
                            158,  # Nephelometer
                            self.model_number,  # Variant
                            0,  # Sub-type
                            0,  # Range
                        )
                    elif command == 2 and not data:
                        await self._write_values(
                            100,  # Build
                            200,  # Branch
                        )
                    elif command == 3:
                        if data != b'REALLY':
                            raise _ErrorResponse(2)
                        # Reset, no response
                        _LOGGER.debug("Instrument reset")
                        await asyncio.sleep(10)
                    elif command == 4:
                        if len(data) % 4 != 0:
                            raise _ErrorResponse(3)
                        response = bytearray()
                        while data:
                            parameter = struct.unpack('>I', data[:4])[0]
                            data = data[4:]
                            response += self._get_value(parameter)
                        await self._write_response(command, response)
                    elif command == 5:
                        if len(data) % 8 != 0:
                            raise _ErrorResponse(3)
                        while data:
                            parameter = struct.unpack('>I', data[:4])[0]
                            self._set_value(parameter, data[4:8])
                            data = data[8:]
                        # No response
                    else:
                        raise _ErrorResponse(1)

                except _ErrorResponse as e:
                    _LOGGER.debug("Invalid packet", exc_info=True)
                    await self._write_response(0, struct.pack('>H', e.error_code))
                except (ValueError, IndexError):
                    _LOGGER.debug("Invalid packet", exc_info=True)
                    await self._write_response(0, struct.pack('>H', 3))
        finally:
            await self._stop_zero()


if __name__ == '__main__':
    from forge.acquisition.serial.simulator import arguments, parse_arguments, run

    parser = arguments()
    parser.add_argument('--polar',
                        dest='polar', action='store_true',
                        help="enable enable polar mode")

    args, _ = parser.parse_known_args()

    run(parse_arguments(), Simulator, polar_mode=args.polar)
