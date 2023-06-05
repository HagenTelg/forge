import typing
import asyncio
import time
from forge.tasks import wait_cancelable
from forge.units import flow_ccm_to_lpm, flow_lpm_to_ccs, pressure_kPa_to_hPa
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number, parse_flags_bits

_INSTRUMENT_TYPE = __name__.split('.')[-2]


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "TSI"
    MODEL = "377x"
    DISPLAY_LETTER = "C"
    TAGS = frozenset({"aerosol", "cpc", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 115200}

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: int = int(context.config.get('REPORT_INTERVAL', default=1))
        self._sleep_time: float = 0.0

        self.data_N = self.input("N")
        self.data_C = self.input("C")
        self.data_P = self.input("P")
        self.data_PDnozzle = self.input("PDnozzle")
        self.data_PDorifice = self.input("PDorifice")
        self.data_Alaser = self.input("Alaser")
        self.data_liquid_level = self.input("liquid_level")

        self.data_Q = self.input("Q")
        self.data_Qinlet = self.input("Qinlet")
        self.data_Qinstrument = self.input("Qinstrument")

        self.data_Tsaturator = self.input("Tsaturator")
        self.data_Tcondenser = self.input("Tcondenser")
        self.data_Toptics = self.input("Toptics")
        self.data_Tcabinet = self.input("Tcabinet")

        if not self.data_N.field.comment and self.data_Q.field.comment:
            self.data_N.field.comment = self.data_Q.field.comment
        if not self.data_N.field.comment and self.data_Qinstrument.field.comment:
            self.data_N.field.comment = self.data_Qinstrument.field.comment

        self.notify_liquid_low = self.notification('liquid_low', is_warning=True)
        self.bit_flags: typing.Dict[int, Instrument.Notification] = {
            0x0040: self.notify_liquid_low,
        }
        self.instrument_report = self.report(
            self.variable_number_concentration(self.data_N, code="N"),

            self.variable_sample_flow(self.data_Q, code="Q",
                                      attributes={'C_format': "%5.3f"}),
            self.variable_flow(self.data_Q, code="Qu", attributes={
                'long_name': "inlet flow rate",
                'C_format': "%5.3f",
            }),

            self.variable_air_pressure(self.data_P, "pressure", code="P",
                                       attributes={'long_name': "absolute pressure"}),
            self.variable_delta_pressure(self.data_PDorifice, "nozzle_pressure_drop", code="Pd1", attributes={
                'long_name': "nozzle pressure drop",
                'C_format': "%6.2f",
            }),
            self.variable_delta_pressure(self.data_PDorifice, "orifice_pressure_drop", code="Pd2", attributes={
                'long_name': "orifice pressure drop",
                'C_format': "%4.0f",
            }),

            self.variable_temperature(self.data_Tsaturator, "saturator_temperature", code="T1",
                                      attributes={'long_name': "saturator block temperature"}),
            self.variable_temperature(self.data_Tcondenser, "condenser_temperature", code="T2",
                                      attributes={'long_name': "condenser temperature"}),
            self.variable_temperature(self.data_Toptics, "optics_temperature", code="T3",
                                      attributes={'long_name': "optics block temperature"}),
            self.variable_temperature(self.data_Tcabinet, "cabinet_temperature", code="T4",
                                      attributes={'long_name': "internal cabinet temperature"}),

            self.variable(self.data_Alaser, "laser_current", code="A", attributes={
                'long_name': "laser current",
                'units': "mA",
                'C_format': "%3.0f"
            }),

            flags=[
                self.flag_bit(self.bit_flags, 0x0001, "saturator_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0002, "condenser_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0004, "optics_temperature_error"),
                self.flag_bit(self.bit_flags, 0x0008, "inlet_flow_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0010, "sample_flow_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0020, "laser_power_error", is_warning=True),
                self.flag(self.notify_liquid_low, preferred_bit=0x0040),
                self.flag_bit(self.bit_flags, 0x0080, "concentration_out_of_range"),
            ],
        )

    async def start_communications(self) -> None:
        if not self.writer:
            raise CommunicationsError

        self.writer.write(b"\r" * 8)
        await self.writer.drain()
        self.writer.write(b"SSTART,0\r")
        await self.writer.drain()
        await self.drain_reader(1.5)

        self.writer.write(b"RMN\r")
        try:
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if data == b"ERROR":
                raise TimeoutError
        except (TimeoutError, asyncio.TimeoutError):
            self.writer.write(b"RMN\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
        if data == b"ERROR":
            raise CommunicationsError
        try:
            model = data.decode('utf-8')
        except UnicodeError as e:
            raise CommunicationsError from e
        self.set_instrument_info('model', model)
        have_coincidence_correction = (model == "3771" or model == "3772")

        self.writer.write(b"RSN\r")
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        if data == b"ERROR":
            raise CommunicationsError
        self.set_serial_number(data)

        self.writer.write(b"RFV\r")
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        if data == b"ERROR":
            raise CommunicationsError
        self.set_firmware_version(data)

        self.writer.write(b"SCD\r")
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        if data != b"ERROR":
            try:
                self.set_instrument_info('calibration', data.decode('utf-8', 'backslashreplace'))
            except UnicodeError as e:
                raise CommunicationsError from e

        self.writer.write(b"RIE\r")
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        try:
            flags = int(data, 16)
            if flags < 0 or flags > 0xFFFF:
                raise ValueError
        except ValueError:
            raise CommunicationsError(f"invalid flags {data}")

        self.writer.write(b"SCM,0\r")
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        if data != b"OK":
            raise CommunicationsError(f"invalid response {data}")

        # JAO email on 2022-03-16 says ACTRIS wants non-corrected, so turn it off
        if have_coincidence_correction:
            self.writer.write(b"SCC,0\r")
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            if data != b"OK":
                raise CommunicationsError(f"invalid response {data}")

        # Do not set the time, because certain types lock up if the clock is changed

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

        self.writer.write(b"RALL\r")
        line: bytes = await wait_cancelable(self.read_line(), 2.0)
        if len(line) < 3:
            raise CommunicationsError

        fields = line.split(b',')
        try:
            (
                N, flags, Tsaturator, Tcondenser, Toptics, Tcabinet,
                P, PDorifice, PDnozzle, Alaser, liquid_level
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        self.writer.write(b"RSF\r")
        Qsample = await wait_cancelable(self.read_line(), 2.0)

        self.writer.write(b"RIF\r")
        Qinlet = await wait_cancelable(self.read_line(), 2.0)

        # JAO email on 2022-03-16 says ACTRIS wants non-corrected, so use RCOUNT2
        self.writer.write(b"RCOUNT2\r")
        C = await wait_cancelable(self.read_line(), 2.0)

        self.data_Tsaturator(parse_number(Tsaturator))
        self.data_Tcondenser(parse_number(Tcondenser))
        self.data_Toptics(parse_number(Toptics))
        self.data_Tcabinet(parse_number(Tcabinet))
        self.data_P(pressure_kPa_to_hPa(parse_number(P)))
        self.data_PDorifice(pressure_kPa_to_hPa(parse_number(PDorifice)))
        self.data_PDnozzle(pressure_kPa_to_hPa(parse_number(PDnozzle)))
        self.data_Alaser(parse_number(Alaser))
        self.data_Qinlet(parse_number(Qinlet))
        C = self.data_C(parse_number(C))

        Qinstrument = self.data_Qinstrument(flow_ccm_to_lpm(parse_number(Qsample)))
        Q = self.data_Q(Qinstrument)

        # From above, make sure we're using uncorrected
        # N = parse_number(N)
        # N *= Qinstrument / Q
        N = C / flow_lpm_to_ccs(Q)
        self.data_N(N)

        parse_flags_bits(flags, self.bit_flags)
        if liquid_level.startswith(b"FULL ("):
            self.notify_liquid_low(False)

            adc_level = liquid_level[6:]
            try:
                adc_level = adc_level[:adc_level.index(b')')]
            except ValueError:
                pass
            try:
                adc_level = int(adc_level)
                if adc_level < 0 or adc_level > 0xFFFF:
                    raise ValueError
                self.data_liquid_level(adc_level)
            except ValueError:
                raise CommunicationsError(f"invalid liquid level {liquid_level}")
        elif liquid_level.startswith(b"NOTFULL ("):
            self.notify_liquid_low(True)

            adc_level = liquid_level[6:]
            try:
                adc_level = adc_level[:adc_level.index(b')')]
            except ValueError:
                pass
            try:
                adc_level = int(adc_level)
                if adc_level < 0 or adc_level > 0xFFFF:
                    raise ValueError
                self.data_liquid_level(adc_level)
            except ValueError:
                raise CommunicationsError(f"invalid liquid level {liquid_level}")
        elif liquid_level == b"FULL":
            self.notify_liquid_low(False)
        elif liquid_level == b"NOTFULL":
            self.notify_liquid_low(True)
        else:
            try:
                adc_level = int(liquid_level)
                if adc_level < 0 or adc_level > 0xFFFF:
                    raise ValueError
                self.data_liquid_level(adc_level)
            except ValueError:
                raise CommunicationsError(f"invalid liquid level {liquid_level}")

        self.instrument_report()
        end_read = time.monotonic()
        self._sleep_time = self._report_interval - (end_read - begin_read)
