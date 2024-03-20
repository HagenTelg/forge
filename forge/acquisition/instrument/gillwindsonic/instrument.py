import typing
import asyncio
import re
from forge.tasks import wait_cancelable
from forge.units import speed_knots_to_ms, speed_mph_to_ms, speed_kph_to_ms, speed_fpm_to_ms
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number

_INSTRUMENT_TYPE = __name__.split('.')[-2]
_RECORD_FRAME = re.compile(b"\x02([^\x03]+)\x03" + rb"\s*([0-9A-Fa-f]{1,2})")


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Gill Instruments"
    MODEL = "Windsonic"
    DISPLAY_LETTER = "I"
    TAGS = frozenset({"met", "aerosol", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600}

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1))
        self._address: bytes = str(context.config.get('ADDRESS', default="Q")).encode('ascii')

        self.data_WS = self.input("WS")
        self.data_WD = self.input("WD")

        self.notify_insufficient_u_samples = self.notification("insufficient_u_samples", is_warning=True)
        self.notify_insufficient_v_samples = self.notification("insufficient_v_samples", is_warning=True)
        self.notify_nvm_checksum_failed = self.notification("nvm_checksum_failed", is_warning=True)
        self.notify_rom_checksum_failed = self.notification("rom_checksum_failed", is_warning=True)

        self.instrument_report = self.report(
            *self.variable_winds(self.data_WS, self.data_WD, code=""),

            flags=[
                self.flag(self.notify_insufficient_u_samples, preferred_bit=0x01),
                self.flag(self.notify_insufficient_v_samples, preferred_bit=0x02),
                self.flag(self.notify_nvm_checksum_failed, preferred_bit=0x08),
                self.flag(self.notify_rom_checksum_failed, preferred_bit=0x10),
            ]
        )

        self.parameters_record = self.context.data.constant_record("parameters")
        self.parameter_unit_configuration = self.parameters_record.string("instrument_parameters", attributes={
            'long_name': "unit configuration response to the D3 command",
        })

    async def start_communications(self) -> None:
        if self.writer:
            # Stop reports
            self.writer.write(b"\r\n*" + self._address + b"\r")
            await self.writer.drain()
            await self.drain_reader(5.0)
            self.writer.write(b"*\r")
            await self.writer.drain()
            await self.drain_reader(5.0)

            # Read serial number
            self.writer.write(b"D1\r")
            data: bytes = await wait_cancelable(self.read_line(), 5.0)
            if data == b"D1":  # Ignore the echo
                data: bytes = await wait_cancelable(self.read_line(), 5.0)
            if len(data) < 3:
                raise CommunicationsError
            self.set_serial_number(data)
            await self.drain_reader(2.0)

            # Read firmware version
            self.writer.write(b"D2\r")
            data: bytes = await wait_cancelable(self.read_line(), 5.0)
            if data == b"D2":  # Ignore the echo
                data: bytes = await wait_cancelable(self.read_line(), 5.0)
            if len(data) < 4:
                raise CommunicationsError
            self.set_firmware_version(data)
            await self.drain_reader(2.0)

            # Set units
            self.writer.write(b"U1\r")
            data: bytes = await wait_cancelable(self.read_line(), 5.0)
            if data != b"U1":
                raise CommunicationsError(f"invalid units {data}")
            await self.drain_reader(2.0)

            # Set output format
            self.writer.write(b"O1\r")
            data: bytes = await wait_cancelable(self.read_line(), 5.0)
            if data != b"O1":
                raise CommunicationsError(f"invalid format {data}")
            await self.drain_reader(2.0)

            # Set message format
            self.writer.write(b"M2\r")
            data: bytes = await wait_cancelable(self.read_line(), 5.0)
            if data != b"M2":
                raise CommunicationsError(f"invalid format {data}")
            await self.drain_reader(2.0)

            # Report rate
            if self._report_interval < 0.5:
                command = b"P3"
            elif self._report_interval < 1.0:
                command = b"P2"
            elif self._report_interval < 2:
                command = b"P1"
            elif self._report_interval < 4:
                command = b"P21"
            else:
                command = b"P20"
            self.writer.write(command + b"\r")
            data: bytes = await wait_cancelable(self.read_line(), 5.0)
            if data != command:
                raise CommunicationsError(f"invalid report rate {data}, expecting {command}")
            await self.drain_reader(2.0)

            # Read configuration
            self.writer.write(b"D3\r")
            data: bytes = await wait_cancelable(self.read_line(), 5.0)
            if data == b"D3":  # Ignore the echo
                data: bytes = await wait_cancelable(self.read_line(), 5.0)
            if len(data) < 4:
                raise CommunicationsError
            self.parameter_unit_configuration(data.decode('utf-8', errors='backslashreplace'))
            await self.drain_reader(2.0)

            self.writer.write(b"Q\r")
            await self.writer.drain()

        # Flush the first record
        await self.drain_reader(5.0)
        await wait_cancelable(self.read_line(), self._report_interval * 2 + 1)

        # Process a valid record
        await self.communicate()

    async def communicate(self) -> None:
        line: bytes = await wait_cancelable(self.read_line(), self._report_interval * 2 + 1)
        if len(line) < 3:
            raise CommunicationsError

        frame = _RECORD_FRAME.fullmatch(line)
        if not frame:
            raise CommunicationsError(f"invalid response {line}")

        try:
            checksum = int(frame.group(2).strip(), 16)
        except ValueError:
            raise CommunicationsError(f"invalid checksum for {line}")

        inner = frame.group(1)
        v = 0
        for b in inner:
            v ^= b
        if v != checksum:
            raise CommunicationsError(f"checksum mismatch on {line} (got {v:02X})")

        inner = inner.strip()
        if inner.endswith(b','):
            inner = inner[:-1]
        fields = inner.split(b',')
        try:
            (
                address, WD, WS, units, status
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        units = units.strip().upper()
        if units == b"M":
            converter = lambda x: x
        elif units == b"N":
            converter = speed_knots_to_ms
        elif units == b"P":
            converter = speed_mph_to_ms
        elif units == b"K":
            converter = speed_kph_to_ms
        elif units == b"F":
            converter = speed_fpm_to_ms
        else:
            raise CommunicationsError(f"unknown units in {line}")

        # Empty seems to happen occasionally (calculation failure?)
        if len(WD.strip()) != 0:
            self.data_WD(parse_number(WD))
        if len(WS.strip()) != 0:
            self.data_WS(converter(parse_number(WS)))

        status = status.strip()
        try:
            if status.upper() == "A" or status.upper() == "V":
                status = 0
            else:
                status = int(status, 16)
        except (ValueError, OverflowError):
            raise CommunicationsError(f"unknown status in {line}")

        self.notify_insufficient_u_samples(status == 0x01 or status == 0x04)
        self.notify_insufficient_v_samples(status == 0x02 or status == 0x04)
        self.notify_nvm_checksum_failed(status == 0x08)
        self.notify_rom_checksum_failed(status == 0x09)

        self.instrument_report()
