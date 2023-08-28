import typing
import asyncio
import logging
import time
import serial
from forge.tasks import wait_cancelable
from forge.units import flow_lpm_to_ccs
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "TSI"
    MODEL = "3010"
    DISPLAY_LETTER = "C"
    TAGS = frozenset({"aerosol", "cpc", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600, 'parity': serial.PARITY_EVEN, 'bytesize': serial.SEVENBITS}

    DEFAULT_FLOW = 1.0

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: int = int(context.config.get('REPORT_INTERVAL', default=1))
        self._sleep_time: float = 0.0
        self._last_nonzero_counts: float = time.monotonic()

        self.data_N = self.input("N")
        self.data_C = self.input("C")
        self.data_Q = self.input("Q", send_to_bus=False)
        self.data_Tsaturator = self.input("Tsaturator")
        self.data_Tcondenser = self.input("Tcondenser")

        if not self.data_N.field.comment and self.data_Q.field.comment:
            self.data_N.field.comment = self.data_Q.field.comment

        self.notify_liquid_low = self.notification('liquid_low', is_warning=True)
        self.notify_vacuum_low = self.notification('vacuum_low', is_warning=True)
        self.notify_not_ready = self.notification('not_ready', is_warning=True)

        self.instrument_report = self.report(
            self.variable_number_concentration(self.data_N, code="N"),
            self.variable_temperature(self.data_Tsaturator, "saturator_temperature", code="T1",
                                      attributes={'long_name': "saturator block temperature"}),
            self.variable_temperature(self.data_Tcondenser, "condenser_temperature", code="T2",
                                      attributes={'long_name': "condenser temperature"}),

            flags=[
                self.flag(self.notify_not_ready, preferred_bit=0x0001),
                self.flag(self.notify_liquid_low, preferred_bit=0x0002),
                self.flag(self.notify_vacuum_low, preferred_bit=0x0004),
            ],
        )

        self._fill_request = False
        self.context.bus.connect_command('fill', self.fill_command)

    def fill_command(self, _) -> None:
        self._fill_request = True

    def _process_data_line(self, line: bytes) -> bool:
        fields = line.split(b',')
        try:
            (count_seconds, number_counts) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        count_seconds = parse_number(count_seconds)
        number_counts = parse_number(number_counts)

        if number_counts < 0.0:
            raise CommunicationsError("negative total counts")
        if count_seconds <= 0.0:
            return False

        C = self.data_C(number_counts / count_seconds)
        Q = self.data_Q(self.DEFAULT_FLOW)
        N = C / flow_lpm_to_ccs(Q)
        self.data_N(N)

        return True

    async def retry_command(self, command: bytes, retry_count: int = 4) -> bytes:
        for _ in range(retry_count):
            self.writer.write(command + b"\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if data == b"ERROR":
                continue
            return data
        raise CommunicationsError(f"retry count exhausted for {command}")

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError

        self.writer.write(b"\r" * 32)
        await self.writer.drain()
        self.writer.write(b"DC\r")
        await self.writer.drain()
        await self.drain_reader(2.0)

        for i in range(4):
            self.writer.write(b"DC\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if data == b"ERROR":
                continue
            if self._process_data_line(data):
                break
            await asyncio.sleep(self._report_interval)
        else:
            raise CommunicationsError

        data: bytes = await self.retry_command(b"R1")
        parse_number(data)

        data: bytes = await self.retry_command(b"R2")
        parse_number(data)

        data: bytes = await self.retry_command(b"RV")
        if data != b"VAC" and data != b"LOVAC":
            raise CommunicationsError(f"invalid vacuum response {data}")

        async def expect_error():
            try:
                data: bytes = await wait_cancelable(self.read_line(), 2.0)
            except (TimeoutError, asyncio.TimeoutError):
                return
            if data != b"ERROR":
                raise CommunicationsError(f"got {data} instead of error response")

        # Disambiguation with other instruments
        self.writer.write(b"I\r")
        await expect_error()
        self.writer.write(b"C\r")
        await expect_error()

        async def wait_for_non_zero():
            while True:
                self.writer.write(b"DC\r")
                data: bytes = await self.read_line()
                if data == b"ERROR":
                    continue
                if self._process_data_line(data):
                    return

        await wait_cancelable(wait_for_non_zero(), self._report_interval * 2 + 4.0)
        self._sleep_time = 0.0
        self._last_nonzero_counts = time.monotonic()

    async def communicate(self) -> None:
        if not self.writer:
            raise CommunicationsError

        if self._fill_request:
            _LOGGER.debug("Issuing instrument fill command")
            self._fill_request = False
            self.writer.write(b"X5\r")
            await self.writer.drain()
            await self.drain_reader(1.0)

        if self._sleep_time > 0.0:
            await asyncio.sleep(self._sleep_time)
            self._sleep_time = 0.0
        begin_read = time.monotonic()

        counts = await self.retry_command(b"DC")
        if self._process_data_line(counts):
            self._last_nonzero_counts = time.monotonic()

            liquid_level = await self.retry_command(b"R0")
            Tcondenser = await self.retry_command(b"R1")
            Tsaturator = await self.retry_command(b"R2")
            system_status = await self.retry_command(b"R5")
            vacuum = await self.retry_command(b"RV")

            if liquid_level == b"FULL":
                self.notify_liquid_low(False)
            elif liquid_level == b"NOTFULL":
                self.notify_liquid_low(True)
            else:
                raise CommunicationsError(f"invalid R0 response {liquid_level}")

            if vacuum == b"VAC":
                self.notify_vacuum_low(False)
            elif vacuum == b"LOVAC":
                self.notify_vacuum_low(True)
            else:
                raise CommunicationsError(f"invalid RV response {vacuum}")

            if system_status == b"READY":
                self.notify_not_ready(False)
            elif system_status == b"NOTREADY":
                self.notify_not_ready(True)
            else:
                raise CommunicationsError(f"invalid R5 response {system_status}")

            self.data_Tcondenser(parse_number(Tcondenser))
            self.data_Tsaturator(parse_number(Tsaturator))

            self.instrument_report()
        else:
            if time.monotonic() - self._last_nonzero_counts > self._report_interval * 2.0:
                raise CommunicationsError("zero count time exhausted")

        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)
