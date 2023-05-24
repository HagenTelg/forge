import typing
import asyncio
import logging
import time
import re
from math import floor
from forge.tasks import wait_cancelable
from forge.units import flow_ccm_to_lpm
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number, parse_flags_bits

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]
_RV_RESPONSE = re.compile(rb"Model\s+3781\s+Ver(?:sion)?\s+(\S+)\s+(?:(?:S/N)|(?:Ser(?:ial)?\s*Num(?:ber)?))\s+(\d+)")


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "TSI"
    MODEL = "3781"
    DISPLAY_LETTER = "C"
    TAGS = frozenset({"aerosol", "cpc", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 115200}

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1.0))
        self._sleep_time: float = 0.0

        self.data_N = self.input("N")
        self.data_C = self.input("C")
        self.data_P = self.input("P")
        self.data_Alaser = self.input("Alaser")
        self.data_PCTnozzle = self.input("PCTnozzle")

        self.data_Q = self.input("Q")
        self.data_Qinstrument = self.input("Qinstrument")

        self.data_Tsaturator = self.input("Tsaturator")
        self.data_Tgrowth = self.input("Tgrowth")
        self.data_Toptics = self.input("Toptics")

        if not self.data_N.field.comment and self.data_Q.field.comment:
            self.data_N.field.comment = self.data_Q.field.comment
        if not self.data_N.field.comment and self.data_Qinstrument.field.comment:
            self.data_N.field.comment = self.data_Qinstrument.field.comment

        self.bit_flags: typing.Dict[int, Instrument.Notification] = dict()
        self.instrument_report = self.report(
            self.variable_number_concentration(self.data_N, code="N"),

            self.variable_sample_flow(self.data_Q, code="Q",
                                      attributes={'C_format': "%5.3f"}),

            self.variable_air_pressure(self.data_P, "pressure", code="P",
                                       attributes={'long_name': "absolute pressure"}),

            self.variable_temperature(self.data_Tsaturator, "saturator_temperature", code="T1",
                                      attributes={'long_name': "saturator temperature"}),
            self.variable_temperature(self.data_Tgrowth, "growth_tube_temperature", code="T2",
                                      attributes={'long_name': "growth tube temperature"}),
            self.variable_temperature(self.data_Toptics, "optics_temperature", code="T3",
                                      attributes={'long_name': "optics block temperature"}),

            self.variable(self.data_Alaser, "laser_current", code="A", attributes={
                'long_name': "laser current",
                'units': "mA",
                'C_format': "%3.0f"
            }),

            self.variable(self.data_PCTnozzle, "nozzle_pressure_drop", code="PCT", attributes={
                'long_name': "normalized pressure drop across the nozzle",
                'units': "%",
                'C_format': "%3.0f"
            }),

            flags=[
                self.flag_bit(self.bit_flags, 0x0001, "concentration_out_of_range"),
                self.flag_bit(self.bit_flags, 0x0002, "sample_flow_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0004, "nozzle_flow_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0008, "pressure_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0010, "temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0020, "warmup"),
                self.flag_bit(self.bit_flags, 0x0040, "tilt_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0080, "laser_current_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0100, "water_valve_open"),
                self.flag_bit(self.bit_flags, 0x0200, "liquid_low", is_warning=True),
            ],
        )

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError

        # Disable automatic reports
        self.writer.write(b"SM,0,0\r")
        await self.writer.drain()
        await self.drain_reader(2.0)

        # Disable internal storage
        self.writer.write(b"LM,0\r")
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        if data != b"OK":
            raise CommunicationsError(f"invalid response {data}")

        self.writer.write(b"RV\r")
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        matched = _RV_RESPONSE.fullmatch(data)
        if not matched:
            raise CommunicationsError(f"invalid version {data}")
        self.set_firmware_version(matched.group(1))
        self.set_serial_number(matched.group(2))

        ts = time.gmtime()
        self.writer.write(f"LC,{ts.tm_year},{ts.tm_mon},{ts.tm_mday},{ts.tm_hour},{ts.tm_min},{ts.tm_sec}\r".encode('ascii'))
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        if data != b"OK":
            raise CommunicationsError(f"invalid response {data}")

        # Set report/average interval
        interval = int(floor(self._report_interval * 10.0))
        if interval < 1:
            interval = 1
        elif interval > 36000:
            interval = 36000
        self.writer.write(f"SM,0,{interval}\r".encode('ascii'))
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        if data != b"OK":
            raise CommunicationsError(f"invalid response {data}")

        self._sleep_time = 0.0
        await self.communicate()
        self._sleep_time = 0.0

    async def communicate(self) -> None:
        if not self.writer:
            raise CommunicationsError

        if self._sleep_time > 0.0:
            await asyncio.sleep(self._sleep_time)
            self._sleep_time = 0.0
        begin_read = time.monotonic()

        self.writer.write(b"RDD\r")
        line: bytes = await wait_cancelable(self.read_line(), 2.0)
        fields = line.split(b',')
        try:
            (
                record_id,
                _,  # Mode
                flags, N,
                _,  # Sample interval
                _,  # Live time,
                C,
                _,  # PM reserved
                _,  # RP reserved
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")
        if record_id != b"D":
            raise CommunicationsError(f"invalid record ID in {line}")

        self.writer.write(b"RRS\r")
        line: bytes = await wait_cancelable(self.read_line(), 2.0)
        fields = line.split(b',')
        try:
            (
                record_id, Q, P, Tsaturator, Tgrowth, Toptics
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")
        if record_id != b"S":
            raise CommunicationsError(f"invalid record ID in {line}")

        self.writer.write(b"RL\r")
        Alaser: bytes = await wait_cancelable(self.read_line(), 2.0)

        self.writer.write(b"RN\r")
        PCTnozzle: bytes = await wait_cancelable(self.read_line(), 2.0)

        self.data_C(parse_number(C))
        self.data_P(parse_number(P))
        self.data_Tsaturator(parse_number(Tsaturator))
        self.data_Tgrowth(parse_number(Tgrowth))
        self.data_Toptics(parse_number(Toptics))
        self.data_Alaser(parse_number(Alaser))
        self.data_PCTnozzle(parse_number(PCTnozzle))

        Qinstrument = self.data_Qinstrument(flow_ccm_to_lpm(parse_number(Q)))
        Q = self.data_Q(Qinstrument)

        N = parse_number(N)
        N *= Qinstrument / Q
        self.data_N(N)

        parse_flags_bits(flags, self.bit_flags)

        self.instrument_report()
        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)
