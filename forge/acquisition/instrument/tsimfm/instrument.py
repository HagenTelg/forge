import typing
import asyncio
import time
from math import nan
from forge.tasks import wait_cancelable
from forge.units import pressure_kPa_to_hPa, ZERO_C_IN_K, ONE_ATM_IN_HPA
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number
from ..record import Report

_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "TSI"
    MODEL = "4000"
    DISPLAY_LETTER = "Q"
    TAGS = frozenset({"aerosol", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 38400}

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: int = int(context.config.get('REPORT_INTERVAL', default=1))
        self._use_extended_query: bool = False
        self._sleep_time: float = 0.0

        self.data_Q = self.input("Q")
        self.data_T = self.input("T")
        self.data_P = self.input("P")
        self.data_U = self.input("U")

        self.flow_var = self.variable_sample_flow(self.data_Q, code="Q", attributes={'C_format': "%6.3f"})
        self.flow_var.data.use_standard_temperature = True
        self.flow_var.data.use_standard_pressure = True

        self.instrument_report = self.report(
            self.flow_var,
            self.variable_air_pressure(self.data_P, "pressure", code="P"),
            self.variable_air_temperature(self.data_T, "temperature", code="T"),
        )
        self.instrument_report.record.data_record.standard_temperature = 0.0
        self.instrument_report.record.data_record.standard_pressure = ONE_ATM_IN_HPA

        self.extended_report: typing.Optional[Report] = None

    def _declare_extended(self) -> None:
        if self.extended_report:
            return

        self.extended_report = self.report(
            self.variable_air_rh(self.data_U, "humidity", code="U"),
        )

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError

        # Stop reports
        self.writer.write(b"\r" * 512)
        await self.writer.drain()
        self.writer.write(b"BREAK\r\r\r")
        await self.writer.drain()
        await asyncio.sleep(self._report_interval + 4.0)
        self.writer.write(b"DAFTP0001\r")
        await self.writer.drain()

        self.writer.write(b"\r" * 512)
        await self.writer.drain()
        await asyncio.sleep(self._report_interval + 4.0)
        self.writer.write(b"DAFTP0001\r")
        await self.writer.drain()

        await self.drain_reader(self._report_interval * 2.0 + 4.0)

        self.writer.write(b"MN\r")
        data: bytes = await wait_cancelable(self.read_line(), 4.0)

        # May have the trailing end of the DAFTP command
        def is_model_number(line: bytes) -> bool:
            if data.startswith(b"ERR"):
                raise CommunicationsError
            if data == b"OK":
                return False
            if len(data) < 5:
                return True
            try:
                (_, _, _) = data.split(b",")
            except ValueError:
                return True
            if data.startswith(b"-") or data[:1].isdigit():
                return False
            return True

        while not is_model_number(data):
            data: bytes = await wait_cancelable(self.read_line(), 4.0)
        self._use_extended_query = data.startswith(b"5")
        self.set_instrument_info('model', data.decode('ascii'))
        if self._use_extended_query:
            self._declare_extended()

        self.writer.write(b"SN\r")
        data: bytes = await wait_cancelable(self.read_line(), 4.0)
        if data == b"OK":
            data: bytes = await wait_cancelable(self.read_line(), 4.0)
        if data.startswith(b"ERR"):
            raise CommunicationsError
        self.set_serial_number(data)

        self.writer.write(b"REV\r")
        data: bytes = await wait_cancelable(self.read_line(), 4.0)
        if data == b"OK":
            data: bytes = await wait_cancelable(self.read_line(), 4.0)
        if data.startswith(b"ERR"):
            raise CommunicationsError
        self.set_firmware_version(data)

        self.writer.write(b"DATE\r")
        data: bytes = await wait_cancelable(self.read_line(), 4.0)
        if data == b"OK":
            data: bytes = await wait_cancelable(self.read_line(), 4.0)
        if data.startswith(b"ERR"):
            raise CommunicationsError
        self.flow_var.data.attributes['instrument_calibration_date'] = data.decode('ascii')

        # Mass flow mode
        self.writer.write(b"SUS\r")
        data: bytes = await wait_cancelable(self.read_line(), 4.0)
        if data != b"OK":
            raise CommunicationsError

        # Disable triggers
        self.writer.write(b"CBT\r")
        data: bytes = await wait_cancelable(self.read_line(), 4.0)
        if data != b"OK":
            raise CommunicationsError
        self.writer.write(b"CET\r")
        data: bytes = await wait_cancelable(self.read_line(), 4.0)
        if data != b"OK":
            raise CommunicationsError

        sample_ms = round(self._report_interval * 1000)
        if sample_ms < 1:
            sample_ms = 1
        elif sample_ms > 1000:
            sample_ms = 1000
        self.writer.write(f"SSR{sample_ms:04d}\r".encode('ascii'))
        data: bytes = await wait_cancelable(self.read_line(), 4.0)
        if data != b"OK":
            raise CommunicationsError

        self._sleep_time = 0.0
        await self.communicate()
        self._sleep_time = 0.0

    async def communicate(self) -> None:
        if self._sleep_time > 0.0:
            await asyncio.sleep(self._sleep_time)
            self._sleep_time = 0.0
        begin_read = time.monotonic()

        async def get_data_record() -> bytes:
            read_wait_time = self._report_interval + 4.0

            begin_query = time.monotonic()
            line: bytes = await wait_cancelable(self.read_line(), read_wait_time)
            if line != b"OK":
                raise CommunicationsError(f"invalid query response {line}")
            remaining = read_wait_time - (time.monotonic() - begin_query)
            if remaining < 1.0:
                remaining = 1.0
            line: bytes = await wait_cancelable(self.read_line(), remaining)
            if len(line) < 3:
                raise CommunicationsError

            return line

        if self._use_extended_query:
            self.writer.write(b"DAFTPHxx0001\r")
            line = await get_data_record()
            fields = line.split(b',')
            try:
                (Q, T, P, U) = fields
            except ValueError:
                raise CommunicationsError(f"invalid number of fields in {line}")
        else:
            self.writer.write(b"DAFTP0001\r")
            line = await get_data_record()
            fields = line.split(b',')
            try:
                (Q, T, P) = fields
            except ValueError:
                raise CommunicationsError(f"invalid number of fields in {line}")
            U = None

        Q = parse_number(Q)
        # TSI uses 70F as standard temperature
        Q *= (ZERO_C_IN_K / (ZERO_C_IN_K + 21.11)) * (1013.0 / ONE_ATM_IN_HPA)
        self.data_Q(Q)

        self.data_P(pressure_kPa_to_hPa(parse_number(P)))
        self.data_T(parse_number(T))
        if U is not None:
            self.data_U(parse_number(U))
        else:
            self.data_U(nan)

        self.instrument_report()
        if self.extended_report:
            self.extended_report()

        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)
