import typing
import asyncio
import time
import re
from forge.tasks import wait_cancelable
from forge.units import flow_ccm_to_lpm
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number, parse_flags_bits

_INSTRUMENT_TYPE = __name__.split('.')[-2]
_FIRMWARE_VERSION_MATCH = re.compile(br'BMI\s*MCPC\s*v(\d+(?:\.\d+)?)')


def _power_fraction(v: bytes, upper_limit: float = 200.0) -> float:
    v = parse_number(v)
    if v < 0.0 or v > upper_limit:
        raise CommunicationsError
    return (v / upper_limit) * 100.0


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "BMI"
    MODEL = "1710"
    DISPLAY_LETTER = "C"
    TAGS = frozenset({"aerosol", "cpc", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 38400}

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: int = int(context.config.get('REPORT_INTERVAL', default=1.0))
        self._sleep_time: float = 0.0

        self.data_N = self.input("N")
        self.data_C = self.input("C")
        self.data_Q = self.input("Q")
        self.data_Qinstrument = self.input("Qinstrument")
        self.data_Qsaturator = self.input("Qsaturator")

        self.data_Tinlet = self.input("Tinlet")
        self.data_Tsaturatorbottom = self.input("Tsaturatorbottom")
        self.data_Tsaturatortop = self.input("Tsaturatortop")
        self.data_Tcondenser = self.input("Tcondenser")
        self.data_Toptics = self.input("Toptics")
        self.data_PCTsaturatorbottom = self.input("PCTsaturatorbottom")
        self.data_PCTsaturatortop = self.input("PCTsaturatortop")
        self.data_PCTcondenser = self.input("PCTcondenser")
        self.data_PCToptics = self.input("PCToptics")
        self.data_PCTsaturatorpump = self.input("PCTsaturatorpump")

        if not self.data_N.field.comment and self.data_Q.field.comment:
            self.data_N.field.comment = self.data_Q.field.comment

        self.bit_flags: typing.Dict[int, Instrument.Notification] = dict()
        self.instrument_report = self.report(
            self.variable_number_concentration(self.data_N, code="N"),
            self.variable_sample_flow(self.data_Q, code="Q1",
                                      attributes={'C_format': "%5.3f"}),
            self.variable_flow(self.data_Qsaturator, "saturator_flow", code="Q2", attributes={
                'C_format': "%5.3f",
                'long_name': "saturator flow",
            }),
            self.variable_air_temperature(self.data_Tinlet, "inlet_temperature", code="Tu",
                                          attributes={'long_name': "air temperature at the instrument inlet"}),
            self.variable_temperature(self.data_Tsaturatorbottom, "saturator_bottom_temperature", code="T1",
                                      attributes={'long_name': "temperature of the bottom of the saturator block"}),
            self.variable_temperature(self.data_Tsaturatortop, "saturator_top_temperature", code="T2",
                                      attributes={'long_name': "temperature of the top of the saturator block"}),
            self.variable_temperature(self.data_Tcondenser, "condenser_temperature", code="T3",
                                      attributes={'long_name': "condenser block temperature"}),
            self.variable_temperature(self.data_Toptics, "optics_temperature", code="T4",
                                      attributes={'long_name': "optics block temperature"}),

            flags=[
                self.flag_bit(self.bit_flags, 0x0001, "eeprom_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0002, "configuration_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0004, "rtc_reset"),
                self.flag_bit(self.bit_flags, 0x0008, "rtc_error"),
                self.flag_bit(self.bit_flags, 0x0010, "sdcard_error"),
                self.flag_bit(self.bit_flags, 0x0020, "sdcard_format_error"),
                self.flag_bit(self.bit_flags, 0x0040, "sdcard_full"),
                self.flag_bit(self.bit_flags, 0x0080, "saturator_pump_warning", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0100, "liquid_low", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0200, "temperature_control_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0400, "overheating", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0800, "optics_thermistor_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x1000, "condenser_thermistor_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x2000, "saturator_top_thermistor_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x4000, "saturator_bottom_thermistor_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x8000, "inlet_thermistor_error", is_warning=True),
            ],
        )

    async def start_communications(self) -> None:
        if self.writer:
            # Stop reports
            self.writer.write(b"autorpt=0\r")
            await self.writer.drain()
            await self.drain_reader(self._report_interval + 1.0)

            self.writer.write(b"rptlabel=1\r")
            await self.writer.drain()
            await self.drain_reader(0.5)

            self.writer.write(b"ver\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            hit = _FIRMWARE_VERSION_MATCH.fullmatch(data)
            if hit:
                self.set_firmware_version(hit.group(1))
            else:
                self.set_firmware_version(data)

            # Flush the first record
            self.writer.write(b"status\r")
            await wait_cancelable(self.read_line(), self._report_interval * 2.0 + 1.0)

        # Process a valid record
        self._sleep_time = 0.0
        await self.communicate()
        self._sleep_time = 0.0

    async def communicate(self) -> None:
        if self.writer and self._sleep_time > 0.0:
            await asyncio.sleep(self._sleep_time)
            self._sleep_time = 0.0
        begin_read = time.monotonic()

        if self.writer:
            self.writer.write(b"status\r")
        lines = await self.read_multiple_lines(total=self._report_interval + 1.0, first=self._report_interval + 1.0,
                                               tail=max(self._report_interval / 3.0, 0.1))

        N: typing.Optional[float] = None
        for line in lines:
            fields: typing.List[bytes] = line.split()
            if len(fields) < 3:
                raise CommunicationsError(f"too few value pairs in {line}")

            for field in fields:
                try:
                    (key, value) = field.split(b'=')
                except ValueError:
                    raise CommunicationsError(f"invalid pair syntax in {field}")

                key = key.lower()
                if key == b"concn":
                    N = parse_number(value)
                elif key == b"count":
                    self.data_C(parse_number(value))
                elif key == b"optct":
                    self.data_Toptics(parse_number(value))
                elif key == b"optcp":
                    self.data_PCToptics(_power_fraction(value))
                elif key == b"condt":
                    self.data_Tcondenser(parse_number(value))
                elif key == b"condp":
                    self.data_PCTcondenser(_power_fraction(value, upper_limit=250.0))
                elif key == b"sattt":
                    self.data_Tsaturatortop(parse_number(value))
                elif key == b"sattp":
                    self.data_PCTsaturatortop(_power_fraction(value))
                elif key == b"satbt":
                    self.data_Tsaturatorbottom(parse_number(value))
                elif key == b"satbp":
                    self.data_PCTsaturatorbottom(_power_fraction(value))
                elif key == b"satfl":
                    self.data_Qsaturator(flow_ccm_to_lpm(parse_number(value)))
                elif key == b"satfp":
                    self.data_PCTsaturatorpump(_power_fraction(value))
                elif key == b"smpfl":
                    self.data_Qinstrument(flow_ccm_to_lpm(parse_number(value)))
                elif key == b"inltt":
                    self.data_Tinlet(parse_number(value))
                elif key == b"errnm":
                    parse_flags_bits(value, self.bit_flags)
                elif key == b"fillc":
                    pass
                elif key == b"rconc":
                    pass
                else:
                    raise CommunicationsError(f"unknown parameter {key}={value}")

        Qinstrument = self.data_Qinstrument.value
        Q = self.data_Q(Qinstrument)

        N *= Qinstrument / Q
        self.data_N(N)

        self.instrument_report()

        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)
