import typing
import asyncio
import re
from forge.tasks import wait_cancelable
from forge.units import flow_lpm_to_ccs
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number

_INSTRUMENT_TYPE = __name__.split('.')[-2]

# Data frames can take two forms:
#  \r\X  1234   1\r\X
#  \r\X\n  1234   1\r\X\n
# Where \X is apparently an uninitialized (random) byte.
_MATCH_LINE = re.compile(rb".? *(\d+ +\d+) *")
_FIELD_SPLIT = re.compile(rb" +")


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "TSI"
    MODEL = "3760"
    DISPLAY_LETTER = "C"
    TAGS = frozenset({"aerosol", "cpc", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600}

    DEFAULT_FLOW = 1.4210

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._second_channel: bool = int(context.config.get('CHANNEL', default=1)) == 2

        self.data_N = self.input("N")
        self.data_C = self.input("C")
        self.data_Q = self.input("Q", send_to_bus=False)

        if not self.data_N.field.comment and self.data_Q.field.comment:
            self.data_N.field.comment = self.data_Q.field.comment

        self.instrument_report = self.report(
            self.variable_number_concentration(self.data_N, code="N"),
        )

    async def _read_frame(self) -> None:
        while True:
            line: bytes = await self.read_line()
            if len(line) == 1:
                continue
            matched = _MATCH_LINE.fullmatch(line)
            if not matched:
                continue
            return matched.group(1)

    async def start_communications(self) -> None:
        # Flush the first record
        await self.drain_reader(0.5)
        await wait_cancelable(self._read_frame(), 3.0)

        # Process a valid record
        await self.communicate()

    async def communicate(self) -> None:
        line: bytes = await wait_cancelable(self._read_frame(), 3.0)
        if len(line) < 3:
            raise CommunicationsError

        fields = _FIELD_SPLIT.split(line.strip())
        if len(fields) != 2:
            raise CommunicationsError(f"invalid number of fields in {line}")

        if self._second_channel:
            C = parse_number(fields[1])
        else:
            C = parse_number(fields[0])
        C = self.data_C(C)
        Q = self.data_Q(self.DEFAULT_FLOW)
        N = C / flow_lpm_to_ccs(Q)
        self.data_N(N)

        self.instrument_report()
