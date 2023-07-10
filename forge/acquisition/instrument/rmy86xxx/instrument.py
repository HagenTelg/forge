import typing
import asyncio
from forge.tasks import wait_cancelable
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number

_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "RMY"
    MODEL = "86xxx"
    DISPLAY_LETTER = "I"
    TAGS = frozenset({"met", "aerosol", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600}

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1))
        self._address: bytes = str(context.config.get('ADDRESS', default="")).encode('ascii')

        self.data_WS = self.input("WS")
        self.data_WD = self.input("WD")

        self.notify_abnormal_status = self.notification("abnormal_status", is_warning=True)

        self.instrument_report = self.report(
            *self.variable_winds(self.data_WS, self.data_WD, code=""),

            flags=[
                self.flag(self.notify_abnormal_status),
            ]
        )

    async def start_communications(self) -> None:
        if self.writer:
            # Stop reports
            self.writer.write(b"\x1B\x1B\x1B\r")
            await self.writer.drain()
            await self.drain_reader(self._report_interval + 1.0)

            # Set ASCII output format
            self.writer.write(b"SET022\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if b"CMD ERR" in data or b"SET" not in data:
                raise CommunicationsError(f"invalid set response {data}")

            # Set m/s units
            self.writer.write(b"SET044\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if b"CMD ERR" in data or b"SET" not in data:
                raise CommunicationsError(f"invalid set response {data}")

            # Set output interval
            report_ms = round(self._report_interval * 1000)
            if report_ms < 1:
                report_ms = 1
            elif report_ms > 9999:
                report_ms = 9999
            self.writer.write(b"SET10%04d\r" % report_ms)
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if b"CMD ERR" in data or b"SET" not in data:
                raise CommunicationsError(f"invalid set response {data}")

            # Set polar output
            self.writer.write(b"SET130\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if b"CMD ERR" in data or b"SET" not in data:
                raise CommunicationsError(f"invalid set response {data}")

            # Set high resolution
            self.writer.write(b"SET151\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if b"CMD ERR" in data or b"SET" not in data:
                raise CommunicationsError(f"invalid set response {data}")

            self.writer.write(b"XX\r")
            await self.writer.drain()

        # Flush the first record
        await self.drain_reader(self._report_interval + 1.0)
        await wait_cancelable(self.read_line(), self._report_interval * 2 + 1)

        # Process a valid record
        await self.communicate()

    async def communicate(self) -> None:
        line: bytes = await wait_cancelable(self.read_line(), self._report_interval * 2 + 1)
        if len(line) < 4:
            raise CommunicationsError

        checksum = line[-3:]
        if checksum[:1] != b"*":
            raise CommunicationsError(f"invalid checksum in {line}")
        checksum = checksum[1:]
        try:
            checksum = int(checksum.strip(), 16)
            if checksum < 0 or checksum > 0xFF:
                raise ValueError
        except ValueError:
            raise CommunicationsError(f"invalid checksum in {line}")

        frame = line[:-3]
        v = 0
        for b in frame:
            v ^= b
        if v != checksum:
            raise CommunicationsError(f"checksum mismatch on {line} (got {v:02X})")

        fields = frame.strip().split()
        try:
            (address, WS, WD, status) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        if self._address and address != self._address:
            raise CommunicationsError(f"address mismatch in {line}")

        self.data_WD(parse_number(WD))
        self.data_WS(parse_number(WS))

        try:
            status = int(status, 16)
        except ValueError:
            raise CommunicationsError(f"invalid status in {line}")

        self.notify_abnormal_status(status != 0)

        self.instrument_report()
