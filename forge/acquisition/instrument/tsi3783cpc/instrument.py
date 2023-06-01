import typing
import asyncio
import logging
import time
import re
from math import floor
from forge.tasks import wait_cancelable
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number, parse_flags_bits, parse_date_and_time

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]
_RV_RESPONSE = re.compile(rb"Model\s+3783\s+Ver(?:sion)?\s+(\S+)\s+(?:(?:S/N)|(?:Ser(?:ial)?\s*Num(?:ber)?))\s+(\d+)")


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "TSI"
    MODEL = "3783"
    DISPLAY_LETTER = "C"
    TAGS = frozenset({"aerosol", "cpc", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 115200}

    DEFAULT_FLOW = 0.12

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=1.0))
        self._sleep_time: float = 0.0

        self.data_N = self.input("N")
        self.data_C = self.input("C")
        self.data_Alaser = self.input("Alaser")
        self.data_PCTnozzle = self.input("PCTnozzle")

        self.data_P = self.input("P")
        self.data_Pvacuum = self.input("Pvacuum")

        self.data_Qinlet = self.input("Qinlet")
        self.data_Q = self.input("Q", send_to_bus=False)

        self.data_Tinlet = self.input("Tinlet")
        self.data_Tsaturator = self.input("Tsaturator")
        self.data_Tgrowth = self.input("Tgrowth")
        self.data_Toptics = self.input("Toptics")
        self.data_Tseparator = self.input("Tseparator")
        self.data_Tcabinet = self.input("Tcabinet")

        self.data_Vphotodetector = self.input("Vphotodetector")
        self.data_Vpulse = self.input("Vpulse")

        if not self.data_N.field.comment and self.data_Q.field.comment:
            self.data_N.field.comment = self.data_Q.field.comment

        self.bit_flags: typing.Dict[int, Instrument.Notification] = dict()
        self.instrument_report = self.report(
            self.variable_number_concentration(self.data_N, code="N"),

            self.variable_flow(self.data_Qinlet, "inlet_flow", code="Qu", attributes={
                'long_name': "inlet flow rate",
                'C_format': "%5.3f"
            }),

            self.variable_air_pressure(self.data_P, "pressure", code="P1",
                                       attributes={'long_name': "absolute pressure at instrument inlet"}),
            self.variable_pressure(self.data_Pvacuum, "vacuum_pressure", code="P2",
                                   attributes={'long_name': "vacuum pressure at instrument outlet"}),

            self.variable_air_temperature(self.data_Tinlet, "inlet_temperature", code="Tu",
                                          attributes={'long_name': "air temperature at the instrument inlet"}),
            self.variable_temperature(self.data_Tsaturator, "saturator_temperature", code="T1",
                                      attributes={'long_name': "saturator temperature"}),
            self.variable_temperature(self.data_Tgrowth, "growth_tube_temperature", code="T2",
                                      attributes={'long_name': "growth tube temperature"}),
            self.variable_temperature(self.data_Toptics, "optics_temperature", code="T3",
                                      attributes={'long_name': "optics block temperature"}),
            self.variable_temperature(self.data_Tseparator, "water_seperator_temperature", code="T4",
                                      attributes={'long_name': "water separator temperature"}),
            self.variable_temperature(self.data_Tcabinet, "cabinet_temperature", code="T5",
                                      attributes={'long_name': "temperature inside the cabinet"}),

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

            self.variable(self.data_Vphotodetector, "photodetector_voltage", code="V1", attributes={
                'long_name': "average photodetector voltage",
                'units': "mV",
                'C_format': "%3.0f"
            }),
            self.variable(self.data_Vpulse, "pulse_height", code="V2", attributes={
                'long_name': "average pulse height",
                'units': "mV",
                'C_format': "%3.0f"
            }),

            flags=[
                self.flag_bit(self.bit_flags, 0x0001, "saturator_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0002, "growth_tube_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0004, "optics_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0008, "vacuum_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0020, "laser_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0040, "liquid_low", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0080, "concentration_out_of_range"),
                self.flag_bit(self.bit_flags, 0x0100, "pulse_height_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0200, "pressure_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0400, "nozzle_pressure_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0800, "seperator_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x1000, "warmup"),
                self.flag_bit(self.bit_flags, 0x4000, "service_reminder"),
            ],
        )

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError

        # Disable automatic reports
        self.writer.write(b"SM,0,0\r")
        await self.writer.drain()
        await self.drain_reader(2.0)

        self.writer.write(b"RV\r")
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        matched = _RV_RESPONSE.fullmatch(data)
        if not matched:
            raise CommunicationsError(f"invalid version {data}")
        self.set_firmware_version(matched.group(1))
        self.set_serial_number(matched.group(2))

        ts = time.gmtime()
        self.writer.write(f"SR,{ts.tm_year},{ts.tm_mon},{ts.tm_mday},{ts.tm_hour},{ts.tm_min},{ts.tm_sec}\r".encode('ascii'))
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
                record_id, raw_date, raw_time,
                flags, N,
                _,  # Elapsed sample time
                _,  # Live time,
                C, Vphotodetector,
                _,  # Reserved
                Vpulse,
                _,  # Pulse height stddev
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")
        if record_id != b"D":
            raise CommunicationsError(f"invalid record ID in {line}")

        # S record has two different definitions in the manual, so avoid using it when possible
        self.writer.write(b"RRS\r")
        line: bytes = await wait_cancelable(self.read_line(), 2.0)
        fields = line.split(b',')
        try:
            (
                record_id,
                _,  # Inlet pressure
                _,  # Vacuum pressure
                _,  # Conditioner/saturator pressure
                _,  # Growth tube temperature
                _,  # Optics temperature
                _,  # Water separator temperature
                Tinlet
            ) = fields
            if record_id != b"S":
                raise CommunicationsError(f"invalid record ID in {line}")
        except ValueError:
            Tinlet = None

        self.writer.write(b"RIS\r")
        line: bytes = await wait_cancelable(self.read_line(), 2.0)
        fields = line.split(b',')
        try:
            (
                _,  # Concentration
                _,  # Livetime %
                _,  # Unused
                P, PCTnozzle, Qinlet,
                _,  # Analog input voltage
                _,  # Pulse height
                Toptics, Tgrowth, Tsaturator, Tseparator,
                _,  # Water filled/not filled
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        self.writer.write(b"RPV\r")
        Pvacuum: bytes = await wait_cancelable(self.read_line(), 2.0)

        self.writer.write(b"RTA\r")
        Tcabinet: bytes = await wait_cancelable(self.read_line(), 2.0)

        self.writer.write(b"RL\r")
        Alaser: bytes = await wait_cancelable(self.read_line(), 2.0)

        parse_date_and_time(raw_date, raw_time, date_separator=b'/')

        self.data_C(parse_number(C))
        self.data_P(parse_number(P))
        self.data_Pvacuum(parse_number(Pvacuum))
        self.data_Qinlet(parse_number(Qinlet))
        if Tinlet is not None:
            self.data_Tinlet(parse_number(Tinlet))
        self.data_Tsaturator(parse_number(Tsaturator))
        self.data_Tgrowth(parse_number(Tgrowth))
        self.data_Toptics(parse_number(Toptics))
        self.data_Tseparator(parse_number(Tseparator))
        self.data_Tcabinet(parse_number(Tcabinet))
        self.data_Alaser(parse_number(Alaser))
        self.data_PCTnozzle(parse_number(PCTnozzle))
        self.data_Vphotodetector(parse_number(Vphotodetector))
        self.data_Vpulse(parse_number(Vpulse))

        Q = self.data_Q(self.DEFAULT_FLOW)

        N = parse_number(N)
        N *= self.DEFAULT_FLOW / Q
        self.data_N(N)

        parse_flags_bits(flags, self.bit_flags)

        self.instrument_report()
        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)
