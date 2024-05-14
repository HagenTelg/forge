import typing
import asyncio
import logging
import time
import datetime
from forge.tasks import wait_cancelable
from forge.units import temperature_k_to_c, flow_ccm_to_lpm, pressure_mmHg_to_hPa
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number

_INSTRUMENT_TYPE = __name__.split('.')[-2]
_LOGGER = logging.getLogger(__name__)


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "2B Tech"
    MODEL = "205"
    DISPLAY_LETTER = "Z"
    TAGS = frozenset({"ozone", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 2400}

    _AVERAGE_TIME = (
        (0, 2.0),
        (1, 20.0),
        (2, 60.0),
        (3, 300.0),
        (4, 3600.0),
    )

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: int = int(context.config.get('REPORT_INTERVAL', default=2))
        self._pressure_is_torr: bool = bool(context.config.get('PRESSURE_IS_TORR', default=False))

        self.data_X = self.input("X")
        self.data_P = self.input("P")
        self.data_T = self.input("T")
        self.data_Q = self.input("Q")

        self.instrument_report = self.report(
            self.variable_ozone(self.data_X, code="X"),
            self.variable_sample_flow(self.data_Q, code="Q",
                                      attributes={'C_format': "%5.3f"}),
            self.variable_air_pressure(self.data_P, "sample_pressure", code="P",
                                       attributes={'long_name': "cell pressure"}),
            self.variable_air_temperature(self.data_T, "sample_temperature", code="T",
                                          attributes={'long_name': "cell temperature"}),
        )

    @staticmethod
    def _parse_datetime(instrument_date: bytes, instrument_time: bytes) -> datetime.datetime:
        try:
            day, month, year = instrument_date.split(b'/')
            hour, minute, second = instrument_time.split(b':')

            year = int(year)
            if 0 <= year <= 99:
                td = time.gmtime()
                current_century = td.tm_year - (td.tm_year % 100)
                year += current_century
                if year > td.tm_year + 50:
                    year -= 100
            if year < 1900 or year > 2999:
                raise CommunicationsError(f"invalid year {year}")
            month = int(month)
            day = int(day)
            hour = int(hour)
            minute = int(minute)
            second = int(second)
            return datetime.datetime(year, month, day, hour, minute, second, tzinfo=datetime.timezone.utc)
        except ValueError as e:
            raise CommunicationsError from e

    async def start_communications(self) -> None:
        if self.writer:
            # Stop reports
            self.writer.write(b"\r\nx\r\n")
            await self.writer.drain()
            await self.drain_reader(max(self._report_interval + 1.0, 45.0))
            self.writer.write(b"m")
            await self.writer.drain()
            await self.drain_reader(0.5)

            async def read_prompt() -> bytes:
                line = bytearray()
                while len(line) < 1024:
                    d = await self.reader.read(1)
                    if not d:
                        break
                    if d == b'>' or d == b'\r' or d == b'\n':
                        line = line.strip()
                        if line:
                            break
                        line.clear()
                        continue
                    line += d
                return bytes(line)

            async def wait_for_menu():
                while True:
                    self.writer.write(b"\r")
                    try:
                        line = await wait_cancelable(read_prompt(), 2.0)
                    except asyncio.TimeoutError:
                        await asyncio.sleep(1.0)
                        continue
                    if line.startswith(b"menu"):
                        break
                await self.drain_reader(1.0)

            await wait_cancelable(wait_for_menu(), max(self._report_interval + 5.0, 60.0))

            # Disable logging
            self.writer.write(b"e")
            await self.writer.drain()
            await self.drain_reader(1.0)
            await wait_cancelable(wait_for_menu(), 5.0)

            # Disable raw A and B output
            self.writer.write(b"v")
            await self.writer.drain()
            await self.drain_reader(1.0)
            await wait_cancelable(wait_for_menu(), 5.0)

            # Disable analog output
            self.writer.write(b"n")
            await self.writer.drain()
            await self.drain_reader(1.0)
            await wait_cancelable(wait_for_menu(), 5.0)

            # Set averaging interval
            self.writer.write(b"a")
            await self.writer.drain()
            await self.drain_reader(1.0)
            average_code = 0
            for code, interval in self._AVERAGE_TIME:
                if interval > self._report_interval:
                    break
                average_code = code
            self.writer.write(f"{average_code}".encode('ascii'))
            await wait_cancelable(wait_for_menu(), 5.0)

            # Set time
            self.writer.write(b"c")
            await self.writer.drain()
            clock_read_time = time.time()
            clock_settings = await self.read_multiple_lines(total=5.0, first=2.0, tail=1.0)
            for idx in range(len(clock_settings)-2):
                if clock_settings[idx] == b"Current Date and Time:":
                    current_date = clock_settings[idx+1]
                    current_time = clock_settings[idx+2]
                    break
            else:
                raise CommunicationsError
            instrument_time = self._parse_datetime(current_date, current_time)
            if abs(instrument_time.timestamp() - clock_read_time) > 10.0:
                _LOGGER.debug("Setting instrument time")

                ts = time.gmtime()
                self.writer.write(b"d")
                await self.writer.drain()
                await self.drain_reader(0.5)
                self.writer.write(f"{ts.tm_mday:02}{ts.tm_mon:02}{ts.tm_year%100:02}".encode('ascii'))
                await self.writer.drain()
                await self.drain_reader(0.5)
                self.writer.write(b"\r")
                await self.writer.drain()
                await self.drain_reader(0.5)
                self.writer.write(b"c")
                await self.writer.drain()
                await self.drain_reader(1.0)
                self.writer.write(b"t")
                await self.writer.drain()
                await self.drain_reader(0.5)
                self.writer.write(f"{ts.tm_hour:02}{ts.tm_min:02}{ts.tm_sec:02}".encode('ascii'))
                await self.writer.drain()
                await self.drain_reader(0.5)
                self.writer.write(b"\r")
            else:
                self.writer.write(b"\r")

            await wait_cancelable(wait_for_menu(), 5.0)

            self.writer.write(b"x\r")
            await self.writer.drain()
            await self.drain_reader(60.0)

        # Flush the first record
        await self.drain_reader(5.0)
        await wait_cancelable(self.read_line(), self._report_interval * 2 + 1)

        # Process a valid record
        await self.communicate()

    async def communicate(self) -> None:
        line: bytes = await wait_cancelable(self.read_line(), self._report_interval * 2 + 1)
        if len(line) < 3:
            raise CommunicationsError

        fields = line.split(b',')
        try:
            (
                X, T, P, Q, *analog, instrument_date, instrument_time
            ) = fields
            if len(analog) != 0 and len(analog) != 3:
                raise ValueError
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        self._parse_datetime(instrument_date, instrument_time)

        T = parse_number(T)
        if T > 150.0:
            T = temperature_k_to_c(T)
        P = parse_number(P)
        if self._pressure_is_torr:
            P = pressure_mmHg_to_hPa(P)

        self.data_X(parse_number(X))
        self.data_T(T)
        self.data_P(P)
        self.data_Q(flow_ccm_to_lpm(parse_number(Q)))

        self.instrument_report()
