import typing
import asyncio
import logging
import time
import serial
from forge.tasks import wait_cancelable
from forge.units import flow_lpm_to_ccs, flow_ccs_to_lpm
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "TSI"
    MODEL = "302x"
    DISPLAY_LETTER = "C"
    TAGS = frozenset({"aerosol", "cpc", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600, 'parity': serial.PARITY_EVEN, 'bytesize': serial.SEVENBITS}

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: int = int(context.config.get('REPORT_INTERVAL', default=1))
        self._sleep_time: float = 0.0
        self._last_nonzero_counts: float = time.monotonic()

        self.data_N = self.input("N")
        self.data_C = self.input("C")
        self.data_Q = self.input("Q")
        self.data_Qinstrument = self.input("Qinstrument")
        self.data_Tsaturator = self.input("Tsaturator")
        self.data_Tcondenser = self.input("Tcondenser")
        self.data_Toptics = self.input("Toptics")

        if not self.data_N.field.comment and self.data_Q.field.comment:
            self.data_N.field.comment = self.data_Q.field.comment

        self.notify_liquid_low = self.notification('liquid_low', is_warning=True)
        self.notify_not_ready = self.notification('not_ready', is_warning=True)

        self.instrument_report = self.report(
            self.variable_number_concentration(self.data_N, code="N"),

            self.variable_sample_flow(self.data_Q, code="Q",
                                      attributes={'C_format': "%5.3f"}),

            self.variable_temperature(self.data_Tsaturator, "saturator_temperature", code="T1",
                                      attributes={'long_name': "saturator block temperature"}),
            self.variable_temperature(self.data_Tcondenser, "condenser_temperature", code="T2",
                                      attributes={'long_name': "condenser temperature"}),
            self.variable_temperature(self.data_Toptics, "optics_temperature", code="T3",
                                      attributes={'long_name': "optics block temperature"}),

            flags=[
                self.flag(self.notify_not_ready, preferred_bit=0x0001),
                self.flag(self.notify_liquid_low, preferred_bit=0x0002),
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

        self.data_C(number_counts / count_seconds)

        return True

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError

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

        self.writer.write(b"R1\r")
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        parse_number(data)

        self.writer.write(b"R2\r")
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        parse_number(data)

        self.writer.write(b"R3\r")
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        parse_number(data)

        self.writer.write(b"I\r")
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        if data == b"ERROR":
            raise CommunicationsError
        self.set_instrument_info('calibration', data.decode('utf-8', 'backslashreplace'))

        self.writer.write(b"C\r")
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        if data == b"ERROR":
            raise CommunicationsError
        await self.drain_reader(0.5)

        async def expect_error():
            try:
                data: bytes = await wait_cancelable(self.read_line(), 2.0)
            except (TimeoutError, asyncio.TimeoutError):
                return
            if data != b"ERROR":
                raise CommunicationsError

        # Disambiguation with other instruments
        self.writer.write(b"RV\r")
        await expect_error()

        async def valid_response():
            try:
                data: bytes = await wait_cancelable(self.read_line(), 2.0)
            except (TimeoutError, asyncio.TimeoutError):
                return None
            if data == b"ERROR":
                return None
            return data

        self.writer.write(b"RC\r")
        data = await valid_response()
        if data:
            parse_number(data)
            self.set_instrument_info('model', "3022")
        else:
            self.set_instrument_info('model', "3025")

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

        self.writer.write(b"DC\r")
        counts: bytes = await wait_cancelable(self.read_line(), 2.0)
        if self._process_data_line(counts):
            self._last_nonzero_counts = time.monotonic()

            self.writer.write(b"R0\r")
            liquid_level: bytes = await wait_cancelable(self.read_line(), 2.0)

            self.writer.write(b"R1\r")
            Tcondenser: bytes = await wait_cancelable(self.read_line(), 2.0)

            self.writer.write(b"R2\r")
            Tsaturator: bytes = await wait_cancelable(self.read_line(), 2.0)

            self.writer.write(b"R3\r")
            Toptics: bytes = await wait_cancelable(self.read_line(), 2.0)

            self.writer.write(b"R4\r")
            Q: bytes = await wait_cancelable(self.read_line(), 2.0)

            self.writer.write(b"R5\r")
            system_status: bytes = await wait_cancelable(self.read_line(), 2.0)

            if liquid_level == b"FULL":
                self.notify_liquid_low(False)
            elif liquid_level == b"NOTFULL":
                self.notify_liquid_low(True)
            else:
                raise CommunicationsError(f"invalid R0 response {liquid_level}")

            if system_status == b"READY":
                self.notify_not_ready(False)
            elif system_status == b"NOTREADY":
                self.notify_not_ready(True)
            else:
                raise CommunicationsError(f"invalid R5 response {system_status}")

            self.data_Tcondenser(parse_number(Tcondenser))
            self.data_Tsaturator(parse_number(Tsaturator))
            self.data_Toptics(parse_number(Toptics))

            Qinstrument = self.data_Qinstrument(flow_ccs_to_lpm(parse_number(Q)))
            Q: float = self.data_Q(Qinstrument)

            C = self.data_C.value
            N = C / flow_lpm_to_ccs(Q)
            self.data_N(N)

            self.instrument_report()
        else:
            if time.monotonic() - self._last_nonzero_counts > self._report_interval * 2.0:
                raise CommunicationsError("zero count time exhausted")

        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)
