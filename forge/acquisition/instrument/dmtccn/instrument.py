import typing
import asyncio
import logging
import time
from math import isfinite
from forge.tasks import wait_cancelable
from forge.units import flow_ccm_to_lpm, flow_lpm_to_ccs
from forge.acquisition import LayeredConfiguration
from forge.acquisition.schedule import Schedule
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError
from ..parse import parse_number, parse_time
from ..variable import VariableLastValid, BaseDataOutput

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]


class _BinVariable(VariableLastValid):
    class Field(BaseDataOutput.Integer):
        def __init__(self, name: str):
            super().__init__(name)
            self.variable = None
            self.template = BaseDataOutput.Field.Template.MEASUREMENT

        @property
        def value(self) -> typing.Optional[int]:
            v = self.variable.value
            if v is None or not isfinite(v):
                return None
            return int(self.variable.value)


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "DMT"
    MODEL = "CCN"
    DISPLAY_LETTER = "N"
    TAGS = frozenset({"aerosol", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600}

    class _TemperatureSchedule(Schedule):
        class Active(Schedule.Active):
            @staticmethod
            def _parse_setpoint(data: typing.Any) -> typing.Optional[float]:
                if data is None:
                    return None
                if isinstance(data, bool) and not data:
                    return None
                return float(data)

            def __init__(self, config: LayeredConfiguration):
                super().__init__(config)

                if isinstance(config, LayeredConfiguration):
                    constant_config = config.constant()
                    if constant_config is not None:
                        self.setpoint = self._parse_setpoint(constant_config)
                    else:
                        self.setpoint = self._parse_setpoint(config.get("DT"))
                elif isinstance(config, dict):
                    self.setpoint = self._parse_setpoint(config.get("DT"))
                else:
                    self.setpoint = self._parse_setpoint(config)

            def __repr__(self) -> str:
                return f"TemperatureSchedule.Active({self.describe_offset()}={self.setpoint})"

        def __init__(self, config: typing.Optional[typing.Union[LayeredConfiguration, str, float, bool]],
                     single_entry: bool = False):
            if not isinstance(config, LayeredConfiguration):
                single_entry = True
            elif not config:
                single_entry = True
            elif config.constant(False):
                single_entry = True
            super().__init__(config, single_entry=single_entry)
            self.constant_temperature = (len(self.active) == 1)

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: float = float(context.config.get('REPORT_INTERVAL', default=2.0))

        temperature_schedule = context.config.section_or_constant('TEMPERATURE_CONTROL')
        self._temperature_schedule: typing.Optional["Instrument._TemperatureSchedule"] = None
        if temperature_schedule:
            self._temperature_schedule = self._TemperatureSchedule(temperature_schedule)

        self.data_N = self.input("N")
        self.data_Ttec1 = self.input("Ttec1")
        self.data_Ttec2 = self.input("Ttec2")
        self.data_Ttec3 = self.input("Ttec3")
        self.data_Tsample = self.input("Tsample")
        self.data_Topc = self.input("Topc")
        self.data_Tinlet = self.input("Tinlet")
        self.data_Tnafion = self.input("Tnafion")
        self.data_DTsetpoint = self.input("DTsetpoint")
        # self.data_DTstddev = self.input("DTstddev")
        self.data_Qinstrument = self.input("Qinstrument")
        self.data_Qsample = self.input("Q")
        self.data_Qsheath = self.input("Qsheath")
        self.data_SSset = self.input("SSset")
        # self.data_SScalc = self.input("SScalc")
        self.data_P = self.input("P")
        self.data_Vmonitor = self.input("Vmonitor")
        self.data_Vvalve = self.input("Vvalve")
        self.data_Alaser = self.input("Alaser")
        self.data_minimum_bin_number = self.input("minimum_bin_number")

        self.data_dN = self.input_array("dN")
        # self.data_dNstable = self.input_array("dNstable", send_to_bus=False)

        self.bit_flags: typing.Dict[int, Instrument.Notification] = dict()

        self.report_C = self.report(
            self.variable_number_concentration(self.data_N, code="N"),
            self.variable_size_distribution_dN(self.data_dN, code="Nb", attributes={
                'long_name': "binned number concentration (dN) with ADC overflow in the final bin"
            }),
            # self.variable_size_distribution_dN(self.data_dNstable, "number_distribution_stable", code="Np",
            #                                    attributes={'long_name': "binned number concentration (dN) with unstable data removed"}),

            _BinVariable(self, self.data_minimum_bin_number, "minimum_bin_number", "ZBin", {
                'long_name': "reported minimum bin setting number",
                'C_format': "%02lld",
            })
        )

        self.notify_instrument_temperature_instability = self.notification('instrument_temperature_instability')
        # self.notify_calculated_temperature_instability = self.notification('calculated_temperature_instability')
        self.notify_safe_mode_active = self.notification('safe_mode_active', is_warning=True)

        self.report_H = self.report(
            self.variable_sample_flow(self.data_Qsample, code="Q1"),
            self.variable_flow(self.data_Qsheath, "sheath_flow", code="Q2",
                               attributes={'long_name': "sheath flow"}),
            self.variable_air_pressure(self.data_P, "sample_pressure", code="P",
                                       attributes={'long_name': "sample pressure"}),

            self.variable_rh(self.data_SSset, "supersaturation_setting", code="U", attributes={
                'long_name': "reported supersaturation from onboard instrument calibration",
                'C_format': "%5.3f"
            }),

            self.variable_air_temperature(self.data_Tinlet, "inlet_temperature", code="Tu",
                                          attributes={'long_name': "inlet temperature"}),
            self.variable_temperature(self.data_Ttec1, "tec1_temperature", code="T1",
                                      attributes={'long_name': "temperature of TEC 1"}),
            self.variable_temperature(self.data_Ttec2, "tec2_temperature", code="T2",
                                      attributes={'long_name': "temperature of TEC 2"}),
            self.variable_temperature(self.data_Ttec3, "tec3_temperature", code="T3",
                                      attributes={'long_name': "temperature of TEC 3"}),
            self.variable_temperature(self.data_Tsample, "sample_temperature", code="T4",
                                      attributes={'long_name': "sample temperature"}),
            self.variable_temperature(self.data_Topc, "opc_temperature", code="T5",
                                      attributes={'long_name': "OPC temperature"}),
            self.variable_temperature(self.data_Tnafion, "nafion_temperature", code="T6",
                                      attributes={'long_name': "nafion temperature"}),

            self.variable(self.data_DTsetpoint, "gradiant_setpoint", code="DT", attributes={
                'long_name': "temperature gradiant setpoint",
                'units': "degC",
                'C_format': "%5.2f",
            }),
            # self.variable(self.data_DTstddev, "gradiant_stddev", code="DTg", attributes={
            #     'long_name': "temperature gradiant standard deviation",
            #     'units': "degC",
            #     'C_format': "%5.2f",
            # }),

            self.variable(self.data_Vmonitor, "first_stage_monitor", code="V1", attributes={
                'long_name': "first stage monitor voltage",
                'units': "V",
                'C_format': "%5.2f",
            }),
            self.variable(self.data_Vvalve, "proportional_valve", code="V2", attributes={
                'long_name': "proportional valve voltage",
                'units': "V",
                'C_format': "%5.2f",
            }),
            self.variable(self.data_Alaser, "laser_current", code="A", attributes={
                'long_name': "laser current",
                'units': "mA",
                'C_format': "%7.2f",
            }),

            flags=[
                self.flag_bit(self.bit_flags, 1, "laser_over_current", is_warning=True),
                self.flag_bit(self.bit_flags, 2, "first_stage_monitor_over_voltage"),
                self.flag_bit(self.bit_flags, 4, "flow_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 8, "temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 16, "sample_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 32, "opc_error", is_warning=True),
                self.flag_bit(self.bit_flags, 64, "ccn_counts_low", is_warning=True),
                self.flag_bit(self.bit_flags, 128, "column_temperature_unstable"),
                self.flag_bit(self.bit_flags, 256, "no_opc_communications", is_warning=True),
                self.flag_bit(self.bit_flags, 512, "duplicate_file"),
                self.flag(self.notify_instrument_temperature_instability),
                # self.flag(self.notify_calculated_temperature_instability),
                self.flag(self.notify_safe_mode_active),
            ],
        )

        # self.report_model = self.report(
        #     self.variable_rh(self.data_SSset, "supersaturation_model", code="Uc", attributes={
        #         'long_name': "supersaturation calculated from a numeric model of the instrument",
        #         'C_format': "%5.3f"
        #     }),
        # )

    async def start_communications(self) -> None:
        # Flush the first record
        await self.drain_reader(0.5)
        await wait_cancelable(self.read_line(), self._report_interval * 3.0 + 1.0)

        # Get both record types
        line: bytes = await wait_cancelable(self.read_line(), self._report_interval * 2.0 + 1.0)
        if line.startswith(b'H'):
            if not self._parse_H(line.split(b',')):
                line: bytes = await wait_cancelable(self.read_line(), self._report_interval * 2.0 + 1.0)
                if not line.startswith(b'C'):
                    raise CommunicationsError
                self._parse_C(line.split(b','), allow_combined=False)
        elif line.startswith(b'C'):
            if not self._parse_C(line.split(b',')):
                line: bytes = await wait_cancelable(self.read_line(), self._report_interval * 2.0 + 1.0)
                if not line.startswith(b'H'):
                    raise CommunicationsError
                self._parse_H(line.split(b','), allow_combined=False)
        else:
            raise CommunicationsError

    def _parse_H(self, fields: typing.List[bytes], allow_combined: bool = True) -> bool:
        try:
            (
                record_id, raw_time, SSset, temp_stable,
                Ttec1, Ttec2, Ttec3, Tsample, Tinlet, Topc, Tnafion,
                Qsample, Qsheath, P, Alaser, Vmonitor, DTsetpoint, Vvalve,
                alarm_bits,
                *tail,
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {fields}")
        if record_id.strip() != b"H":
            raise CommunicationsError(f"invalid record ID in {fields}")
        if not allow_combined and len(tail) > 0:
            raise CommunicationsError(f"invalid number of fields in {fields}")
        combined: bool = False
        if len(tail) > 0:
            self._parse_C(tail, allow_combined=False)
            combined = True

        parse_time(raw_time.strip())

        self.notify_instrument_temperature_instability(parse_number(temp_stable) == 0.0)

        self.data_SSset(parse_number(SSset))
        self.data_Ttec1(parse_number(Ttec1))
        self.data_Ttec2(parse_number(Ttec2))
        self.data_Ttec3(parse_number(Ttec3))
        self.data_Tsample(parse_number(Tsample))
        self.data_Tinlet(parse_number(Tinlet))
        self.data_Topc(parse_number(Topc))
        self.data_Tnafion(parse_number(Tnafion))
        self.data_Qsheath(flow_ccm_to_lpm(parse_number(Qsheath)))
        self.data_P(parse_number(P))
        self.data_Alaser(parse_number(Alaser))
        self.data_Vmonitor(parse_number(Vmonitor))
        self.data_DTsetpoint(parse_number(DTsetpoint))
        self.data_Vvalve(parse_number(Vvalve))

        Qinstrument = self.data_Qinstrument(flow_ccm_to_lpm(parse_number(Qsample)))
        self.data_Qsample(Qinstrument)

        alarm_bits = alarm_bits.strip()
        try:
            alarm_bits = int(alarm_bits)
        except ValueError:
            try:
                alarm_bits = int(parse_number(alarm_bits))
            except ValueError:
                raise CommunicationsError(f"invalid alarms in {fields}")
        if alarm_bits < 0:
            self.notify_safe_mode_active(True)
            alarm_bits = -alarm_bits
        else:
            self.notify_safe_mode_active(False)
        for bit, flag in self.bit_flags.items():
            flag((alarm_bits & bit) != 0)

        self.report_H()
        return combined

    def _parse_C(self, fields: typing.List[bytes], allow_combined: bool = True) -> bool:
        try:
            (
                record_id, bin_number, N, adc_overflow,
                *tail,
            ) = fields
            counts = tail[:20]
            counts.append(adc_overflow)
            if len(counts) != 21:
                raise ValueError
            del tail[:20]
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {fields}")
        if record_id.strip() != b"C":
            raise CommunicationsError(f"invalid record ID in {fields}")
        if not allow_combined and len(tail) > 0:
            raise CommunicationsError(f"invalid number of fields in {fields}")
        combined: bool = False
        if len(tail) > 0:
            self._parse_H(tail, allow_combined=False)
            combined = True

        N = parse_number(N)
        N *= self.data_Qinstrument.value / self.data_Qsample.value
        self.data_N(N)

        Q_ccs = flow_lpm_to_ccs(self.data_Qsample.value)
        dN: typing.List[float] = list()
        for c in counts:
            c = parse_number(c)
            c /= Q_ccs
            dN.append(c)
        self.data_dN(dN)

        self.data_minimum_bin_number(float(int(parse_number(bin_number))))

        self.report_C()
        return combined

    async def communicate(self) -> None:
        line: bytes = await wait_cancelable(self.read_line(), self._report_interval * 2.0 + 1.0)
        if len(line) < 3:
            raise CommunicationsError

        if line.startswith(b'H'):
            self._parse_H(line.split(b','))
        elif line.startswith(b'C'):
            self._parse_C(line.split(b','))
        else:
            raise CommunicationsError(f"invalid record in {line}")

        if self.writer and self._temperature_schedule:
            now = time.time()
            setpoint: "Instrument._TemperatureSchedule.Active" = self._temperature_schedule.current(now)
            if setpoint.activate(now):
                value = setpoint.setpoint
                if value is not None and 0.0 <= value <= 99.99:
                    _LOGGER.debug(f"Changing dT setpoint to {value}")
                    self.writer.write(b"%05.2f" % value)
