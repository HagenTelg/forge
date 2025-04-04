import typing
import logging
import asyncio
import time
import enum
import re
from math import nan, isfinite
from forge.tasks import wait_cancelable
from forge.units import ONE_ATM_IN_HPA
from forge.acquisition import LayeredConfiguration
from forge.acquisition.schedule import Schedule
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError, BaseBusInterface
from ..parse import parse_number, parse_time, parse_flags_bits
from ..state import Persistent, ChangeEvent
from ..variable import Input
from ..array import ArrayInput
from ..spancheck import Spancheck as SpancheckController
from ..record import Report

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]
_INSTRUMENT_ID = re.compile(rb"Aurora\s*(\d+).*v([^\s#,]+).*#(.+)$")
_EE_SPACE_REMOVE = re.compile(rb"\s+")


class InstrumentBusy(CommunicationsError):
    pass


def _parse_number_limit(raw: bytes,
                        minimum_acceptable: typing.Optional[float] = None,
                        maximum_acceptable: typing.Optional[float] = None) -> float:
    v = parse_number(raw)
    if minimum_acceptable is not None and v < minimum_acceptable:
        raise CommunicationsError(f"value {v} less than {minimum_acceptable}")
    if maximum_acceptable is not None and v > maximum_acceptable:
        raise CommunicationsError(f"value {v} greater than {maximum_acceptable}")
    return v


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "Ecotech"
    MODEL = "Aurora"
    DISPLAY_LETTER = "N"
    TAGS = frozenset({"aerosol", "scattering", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 38400}

    WAVELENGTHS = (
        (450.0, "B"),
        (525.0, "G"),
        (635.0, "R"),
    )

    @enum.unique
    class SamplingMode(enum.IntEnum):
        Normal = 0
        Zero = 1
        Blank = 2
        Spancheck = 3
        Calibration = 4

    @enum.unique
    class MajorState(enum.IntEnum):
        Normal = 0
        SpanCalibration = 1
        ZeroCalibration = 2
        SpanCheck = 3
        ZeroCheck = 4
        ZeroAdjust = 5
        SystemCalibration = 6
        EnvironmentalCalibration = 7

    class _NormalMinorState(enum.IntEnum):
        ShutterDown = 0
        ShutterMeasure = 1
        ShutterUp = 2
        Measure = 3

    class _ControlZeroState(enum.Enum):
        Idle = enum.auto()
        FillZero = enum.auto()
        MeasureZero = enum.auto()
        FlushAir = enum.auto()

    class _VI(enum.IntEnum):
        SystemState = 0
        SystemFlags = 88

        DataLine = 99

        DarkCounts = 4
        
        Red_ReferenceCounts = 6
        Red_TotalCounts = 7
        Red_Ratio = 8
        Red_BackCounts = 33
        Red_BackRatio = 34
        
        Green_ReferenceCounts = 9
        Green_TotalCounts = 10
        Green_Ratio = 11
        Green_BackCounts = 35
        Green_BackRatio = 36
        
        Blue_ReferenceCounts = 12
        Blue_TotalCounts = 13
        Blue_Ratio = 14
        Blue_BackCounts = 37
        Blue_BackRatio = 38

        @classmethod
        def Cf(cls, wavelength_index: int) -> "Instrument._VI":
            if wavelength_index == 0:
                return cls.Blue_ReferenceCounts
            elif wavelength_index == 1:
                return cls.Green_ReferenceCounts
            elif wavelength_index == 2:
                return cls.Red_ReferenceCounts
            raise ValueError

        @classmethod
        def Cs(cls, wavelength_index: int) -> "Instrument._VI":
            if wavelength_index == 0:
                return cls.Blue_TotalCounts
            elif wavelength_index == 1:
                return cls.Green_TotalCounts
            elif wavelength_index == 2:
                return cls.Red_TotalCounts
            raise ValueError

        @classmethod
        def Cr(cls, wavelength_index: int) -> "Instrument._VI":
            if wavelength_index == 0:
                return cls.Blue_Ratio
            elif wavelength_index == 1:
                return cls.Green_Ratio
            elif wavelength_index == 2:
                return cls.Red_Ratio
            raise ValueError

        @classmethod
        def Cbs(cls, wavelength_index: int) -> "Instrument._VI":
            if wavelength_index == 0:
                return cls.Blue_BackCounts
            elif wavelength_index == 1:
                return cls.Green_BackCounts
            elif wavelength_index == 2:
                return cls.Red_BackCounts
            raise ValueError

        @classmethod
        def Cbr(cls, wavelength_index: int) -> "Instrument._VI":
            if wavelength_index == 0:
                return cls.Blue_BackRatio
            elif wavelength_index == 1:
                return cls.Green_BackRatio
            elif wavelength_index == 2:
                return cls.Red_BackRatio
            raise ValueError

    class _Spancheck(SpancheckController):
        class Angle(SpancheckController.Angle):
            def __init__(self, spancheck: "Instrument._Spancheck"):
                super().__init__(spancheck)
                self.spancheck = spancheck
                self.measurement_counts = spancheck.MeasurementAverage()

        class Wavelength(SpancheckController.Wavelength):
            def __init__(self, spancheck: "Instrument._Spancheck",
                         angles: typing.Dict[float, "Instrument._Spancheck.Angle"]):
                super().__init__(spancheck,  angles)
                self.spancheck = spancheck
                self.reference_counts = spancheck.MeasurementAverage()

        class Phase(SpancheckController.Phase):
            def __init__(self, spancheck: "Instrument._Spancheck",
                         wavelengths: typing.Dict[float, "Instrument._Spancheck.Wavelength"]):
                super().__init__(spancheck, wavelengths)
                self.spancheck = spancheck
                self.dark_counts = spancheck.MeasurementAverage()

        class Result(SpancheckController.Result):
            class ForAngleWavelength(SpancheckController.Result.ForAngleWavelength):
                def output_data(self) -> typing.Dict[str, typing.Dict[str, typing.Any]]:
                    result: "Instrument._Spancheck.Result" = self.result
                    data: typing.Dict[str, typing.Dict[str, typing.Any]] = dict()
                    for i in range(len(self.result.angles)):
                        contents: typing.Dict[str, typing.Any] = dict()
                        angle = self.result.angles[i]
                        if result.is_polar:
                            data[str(int(angle))] = contents
                        if angle == result.angle_total:
                            data['total'] = contents
                        if angle == result.angle_back:
                            data['back'] = contents
                        for j in range(len(self.result.wavelengths)):
                            contents[Instrument.WAVELENGTHS[j][1]] = self.values[i][j]
                    return data

            class ForWavelength(SpancheckController.Result.ForWavelength):
                def output_data(self) -> typing.Dict[str, typing.Any]:
                    data: typing.Dict[str, typing.Any] = dict()
                    for i in range(len(self.result.wavelengths)):
                        data[Instrument.WAVELENGTHS[i][1]] = self.values[i]
                    return data

            def _calculate_sensitivity_factor(self,
                                              air_rayleigh: float, air_measure: float,
                                              gas_rayleigh: float, gas_measure: float) -> float:
                ratio = gas_rayleigh / air_rayleigh - 1.0
                if not isfinite(ratio) or abs(ratio) < 0.01:
                    return nan
                c_gas = self.to_stp(gas_measure, self.gas_density)
                c_air = self.to_stp(air_measure, self.air_density)
                return (c_gas - c_air) / ratio

            def _calculate_calM(self, angle: float, wavelength: float) -> float:
                air_mr = self.air_measurement_ratio[angle, wavelength]
                air_x = self.air_rayleigh_scattering[angle, wavelength] * self.air_density
                gas_mr = self.air_measurement_ratio[angle, wavelength]
                gas_x = self.gas_rayleigh_scattering[angle, wavelength] * self.gas_density

                div = gas_x - air_x
                if not isfinite(div) or div == 0.0:
                    return nan
                return (gas_mr - air_mr) / div

            def _calculate_calC(self, angle: float, wavelength: float) -> float:
                gas_mr = self.air_measurement_ratio[angle, wavelength]
                gas_x = self.gas_rayleigh_scattering[angle, wavelength] * self.gas_density
                calM = self.calM[angle, wavelength]
                return gas_mr - calM * gas_x

            def __init__(self, spancheck: "Instrument._Spancheck", air: "Instrument._Spancheck.Phase",
                         gas: "Instrument._Spancheck.Phase", gas_rayleigh_factor: float):
                report_t = spancheck.instrument.instrument_report.record.data_record.standard_temperature
                report_p = spancheck.instrument.instrument_report.record.data_record.standard_pressure
                super().__init__(spancheck, air, gas, gas_rayleigh_factor,
                                 measurement_stp_t=report_t,
                                 measurement_stp_p=report_p)
                self.is_polar = spancheck.instrument._is_polar
                self.angle_total = spancheck.instrument._total_scattering_angle
                self.angle_back = spancheck.instrument._back_scattering_angle

                self.air_dark_counts = float(self.air.dark_counts)
                self.air_reference_counts = self.ForWavelength(self, lambda wavelength: (
                    float(self.air.wavelengths[wavelength].reference_counts)
                ))
                self.air_measurement_counts = self.ForAngleWavelength(self, lambda angle, wavelength: (
                    float(self.air.wavelengths[wavelength].angles[angle].measurement_counts)
                ))
                self.air_measurement_ratio = self.ForAngleWavelength(self, lambda angle, wavelength: (
                    self.air_measurement_counts[angle, wavelength] / self.air_reference_counts[wavelength]
                ))

                self.gas_dark_counts = float(self.gas.dark_counts)
                self.gas_reference_counts = self.ForWavelength(self, lambda wavelength: (
                    float(self.gas.wavelengths[wavelength].reference_counts)
                ))
                self.gas_measurement_counts = self.ForAngleWavelength(self, lambda angle, wavelength: (
                    float(self.gas.wavelengths[wavelength].angles[angle].measurement_counts)
                ))
                self.gas_measurement_ratio = self.ForAngleWavelength(self, lambda angle, wavelength: (
                    self.gas_measurement_counts[angle, wavelength] / self.gas_reference_counts[wavelength]
                ))

                self.sensitivity_factor = self.ForAngleWavelength(self, lambda angle, wavelength: self._calculate_sensitivity_factor(
                    self.air_rayleigh_scattering[angle, wavelength],
                    self.air_measurement_counts[angle, wavelength],
                    self.gas_rayleigh_scattering[angle, wavelength],
                    self.gas_measurement_counts[angle, wavelength],
                ))

                self.calM = self.ForAngleWavelength(self, self._calculate_calM)
                self.calC = self.ForAngleWavelength(self, self._calculate_calC)

                result_data = self.output_data()
                average_error = self.average_percent_error()
                result_data['percent_error']['average'] = average_error
                spancheck.instrument.data_spancheck_result(result_data, oneshot=True)
                spancheck.instrument.context.bus.log(
                    f"Spancheck completed with an average error of {average_error:.1f}%",
                    result_data)

                spancheck.instrument.data_PCTnc(self.percent_error.values, oneshot=True)

                if self.angle_total is not None:
                    try:
                        spancheck.instrument.data_PCTc([
                            self.percent_error[self.angle_total, wavelength] for wavelength in self.wavelengths
                        ])
                        spancheck.instrument.data_Cc([
                            self.sensitivity_factor[self.angle_total, wavelength] for wavelength in self.wavelengths
                        ])
                    except IndexError:
                        pass

                if self.angle_back is not None:
                    try:
                        spancheck.instrument.data_PCTbc([
                            self.percent_error[self.angle_back, wavelength] for wavelength in self.wavelengths
                        ])
                        spancheck.instrument.data_Cbc([
                            self.sensitivity_factor[self.angle_back, wavelength] for wavelength in self.wavelengths
                        ])
                    except IndexError:
                        pass

            def output_data(self) -> typing.Dict[str, typing.Any]:
                data = super().output_data()
                data['sensitivity_factor'] = self.sensitivity_factor.output_data()
                data['calibration'] = {
                    'C': self.calC.output_data(),
                    'M': self.calM.output_data(),
                }
                data['counts'] = {
                    'air': {
                        'measurement': self.air_measurement_counts.output_data(),
                        'reference': self.air_reference_counts.output_data(),
                        'dark': self.air_dark_counts,
                    },
                    'gas': {
                        'measurement': self.gas_measurement_counts.output_data(),
                        'reference': self.gas_reference_counts.output_data(),
                        'dark': self.gas_dark_counts,
                    },
                }
                return data

        def __init__(self, bus: BaseBusInterface, instrument: "Instrument"):
            super().__init__(bus)
            self.instrument = instrument

        @property
        def wavelengths(self) -> typing.Iterable[float]:
            return self.instrument.data_wavelength.value

        @property
        def angles(self) -> typing.Iterable[float]:
            return self.instrument.data_angle.value

        async def set_filtered_air(self) -> None:
            await self.bus.set_bypass_held(False)
            if not self.instrument.writer:
                return
            await self.instrument._set_digital_span(False)
            await self.instrument._set_digital_zero(True)

        async def set_span_gas(self) -> None:
            await self.bus.set_bypass_held(True)
            if not self.instrument.writer:
                return
            await self.instrument._set_digital_zero(False)
            await self.instrument._set_digital_span(True)

        def abort_desynchronized(self) -> bool:
            if not super().abort_desynchronized():
                return False
            self.instrument.notify_spancheck(False)
            return True

        async def initialize(self) -> None:
            self.instrument.notify_spancheck(True)

        async def abort(self) -> None:
            self.instrument.notify_spancheck(False)
            await self.bus.set_bypass_held(False)
            self.instrument._zero_request = True
            if not self.instrument.writer:
                return
            await self.instrument._set_digital_zero(False)
            await self.instrument._set_digital_span(False)

        async def complete(self) -> None:
            self.instrument.notify_spancheck(False)
            await self.bus.set_bypass_held(False)
            self.instrument._zero_request = True
            if not self.instrument.writer:
                return
            await self.instrument._set_digital_zero(False)
            await self.instrument._set_digital_span(False)

    class _FilterMode(enum.Enum):
        Disabled = "none"
        Kalman = "kalman"
        Average = "average"

        @property
        def command_code(self) -> str:
            if self == Instrument._FilterMode.Kalman:
                return "K"
            elif self == Instrument._FilterMode.Average:
                return "M"
            return "N"

    class _ZeroMode(enum.Enum):
        Native = "native"
        SetFilter = "filter"
        Offset = "offset"

    class _ZeroAverage:
        def __init__(self):
            self.sum: float = 0.0
            self.count: int = 0

        def __call__(self, add: float) -> None:
            if add is None or not isfinite(add):
                return
            self.sum += add
            self.count += 1

        def mean(self) -> float:
            if not self.count:
                return nan
            return self.sum / float(self.count)

        def reset(self) -> None:
            self.sum = 0.0
            self.count = 0

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: typing.Optional[float] = context.config.get('REPORT_INTERVAL')
        self._retry_delay: float = float(context.config.get('RETRY.DELAY', default=2.0))
        self._retry_maximum: int = int(context.config.get('RETRY.COUNT', default=5))
        self._busy_timeout: float = float(context.config.get('BUSY_TIMEOUT', default=1800.0))
        self._command_retry: int = int(context.config.get('COMMAND_RETRY', default=5))
        self._polar_mode: typing.Optional[bool] = context.config.get('POLAR')
        self._enable_backscatter: typing.Optional[bool] = context.config.get('BACKSCATTER')
        self._address: int = int(context.config.get('ADDRESS', default=0))

        self._air_flush_time: float = float(context.config.get('FLUSH_TIME', default=62.0))
        self._zero_fill_time: float = float(context.config.get('OFFSET_FILL', default=240.0))
        self._zero_measure_time: float = float(context.config.get('OFFSET_MEASURE', default=300.0))

        self._filter_mode: typing.Optional[Instrument._FilterMode] = self._FilterMode.Kalman
        filter_mode = context.config.get('FILTER_MODE')
        if filter_mode:
            self._filter_mode = self._FilterMode(str(filter_mode).lower())
        self._can_set_filter_mode: bool = True

        self._zero_mode: Instrument._ZeroMode = self._ZeroMode.Offset
        zero_mode = context.config.get('ZERO_MODE')
        if zero_mode:
            self._zero_mode = self._ZeroMode(str(zero_mode).lower())

        command_delay: float = float(context.config.get('COMMAND_DELAY', default=0.125))
        if command_delay > 0.0:
            async def delay():
                await asyncio.sleep(command_delay)
            self._command_delay = delay
        else:
            async def delay():
                pass
            self._command_delay = delay

        self._have_system_flags: bool = True
        self._sleep_time: float = 0.0
        self._instrument_update_time: float = 3.0
        self._instrument_polar: bool = self._polar_mode
        if self._polar_mode:
            self._instrument_update_time = 60.0
        if self._report_interval:
            self._instrument_update_time = self._report_interval

        self._zero_schedule: typing.Optional[Schedule] = None
        zeros = context.config.section_or_constant('ZERO')
        if not zeros and not isinstance(zeros, bool):
            if self._zero_mode == self._ZeroMode.Offset:
                self._zero_schedule = Schedule(LayeredConfiguration({
                    'SCHEDULE': {'TIME': 'PT54M59S'},
                }))
            else:
                self._zero_schedule = Schedule(LayeredConfiguration({
                    'SCHEDULE': {'TIME': 'PT23H59M58S'},
                    'CYCLE_TIME': 24 * 60 * 60,
                }))
        elif zeros:
            self._zero_schedule = Schedule(zeros)

        self._control_zero_state: Instrument._ControlZeroState = self._ControlZeroState.Idle
        self._control_zero_advance: float = time.monotonic()

        self.data_Tsample = self.input("Tsample")
        self.data_Usample = self.input("Usample")
        self.data_Psample = self.input("Psample")
        self.data_Tcell = self.input("Tcell")
        self.data_Cd = self.input("Cd")

        self.data_major_state = self.persistent_enum("major_state", self.MajorState, send_to_bus=False)
        self.data_sampling = self.persistent_enum("sampling", self.SamplingMode, send_to_bus=False)

        self.data_Tzero = self.persistent("Tzero")
        self.average_Tzero = self._ZeroAverage()
        self.data_Pzero = self.persistent("Pzero")
        self.average_Pzero = self._ZeroAverage()
        self.average_Bszero: typing.List[typing.List["Instrument._ZeroAverage"]] = list()
        self._parameter_change_pending: bool = False
        self._instrument_zero_pending: bool = False
        self._filter_mode_reset_needed: typing.Optional[int] = None

        self.data_Bs_wavelength: typing.List[Input] = list()
        self.data_Bbs_wavelength: typing.List[Input] = list()
        self.data_Bsn_wavelength: typing.List[ArrayInput] = list()
        self.data_Bsw_wavelength: typing.List[Persistent] = list()
        self.data_Bbsw_wavelength: typing.List[Persistent] = list()
        self.data_Bswd_wavelength: typing.List[Persistent] = list()
        self.data_Bbswd_wavelength: typing.List[Persistent] = list()
        self.data_Bsnw_wavelength: typing.List[Persistent] = list()
        self.data_Cs_wavelength: typing.List[Input] = list()
        self.data_Cbs_wavelength: typing.List[Input] = list()
        self.data_Cf_wavelength: typing.List[Input] = list()
        self.data_Cr_wavelength: typing.List[Input] = list()
        self.data_Cbr_wavelength: typing.List[Input] = list()
        for _, code in self.WAVELENGTHS:
            self.data_Bs_wavelength.append(self.input("Bs" + code))
            self.data_Bbs_wavelength.append(self.input("Bbs" + code))
            self.data_Bsn_wavelength.append(self.input_array("Bsn" + code, send_to_bus=False))
            self.data_Bsw_wavelength.append(self.persistent("Bsw" + code))
            self.data_Bbsw_wavelength.append(self.persistent("Bbsw" + code))
            self.data_Bswd_wavelength.append(self.persistent("Bswd" + code))
            self.data_Bbswd_wavelength.append(self.persistent("Bbswd" + code))
            self.data_Bsnw_wavelength.append(self.persistent("Bsnw" + code, send_to_bus=False))
            self.data_Cs_wavelength.append(self.input("Cs" + code))
            self.data_Cbs_wavelength.append(self.input("Cbs" + code))
            self.data_Cf_wavelength.append(self.input("Cf" + code))
            self.data_Cr_wavelength.append(self.input("Cr" + code))
            self.data_Cbr_wavelength.append(self.input("Cbr" + code))

        self.data_angle = self.persistent("angle", save_value=False, send_to_bus=False)
        self.data_angle([0.0, 90.0])
        self.data_wavelength = self.persistent("wavelength", save_value=False, send_to_bus=False)
        self.data_wavelength([wl for wl, _ in self.WAVELENGTHS])
        self.data_Bs = self.input_array("Bs")  # Sent to the bus because it has the zero data removed
        self.data_Bbs = self.input_array("Bbs")
        self.data_Bsn = self.input_array("Bsn", send_to_bus=False, dimensions=2)
        self.data_Bsw = self.persistent("Bsw", send_to_bus=False)
        self.data_Bbsw = self.persistent("Bbsw", send_to_bus=False)
        self.data_Bsnw = self.persistent("Bsnw", send_to_bus=False)
        self.data_Cs = self.input_array("Cs", send_to_bus=False)
        self.data_Cbs = self.input_array("Cbs", send_to_bus=False)
        self.data_Cr = self.input_array("Cr", send_to_bus=False)
        self.data_Cbr = self.input_array("Cbr", send_to_bus=False)
        self.data_Cf = self.input_array("Cf", send_to_bus=False)
        self.data_PCTc = self.persistent("PCTc", send_to_bus=False)
        self.data_PCTbc = self.persistent("PCTbc", send_to_bus=False)
        self.data_PCTnc = self.persistent("PCTnc", send_to_bus=False)
        self.data_Cc = self.persistent("Cc", send_to_bus=False)
        self.data_Cbc = self.persistent("Cbc", send_to_bus=False)
        self.data_Bsnw_base = self.persistent("Bsnwx", send_to_bus=False)
        self.data_Bsnw_offset = self.persistent("Bsnwxd", send_to_bus=False)

        self.notify_blank = self.notification("blank")
        self.notify_zero = self.notification("zero")
        self.notify_spancheck = self.notification("spancheck")
        self.notify_calibration = self.notification("calibration")
        self.notify_inconsistent_zero = self.notification("inconsistent_zero", is_warning=True)

        self.data_spancheck_result = self.persistent("spancheck_result")
        self._spancheck = self._Spancheck(self.context.bus, self)
        self._apply_spancheck_calibration_request: bool = False
        self.context.bus.connect_command('apply_spancheck_calibration', self.apply_spancheck_calibration)

        self.dimension_wavelength = self.dimension_wavelength(self.data_wavelength)
        self.bit_flags: typing.Dict[int, Instrument.Notification] = dict()

        self._instrument_stp_variables: typing.List[Instrument.Variable] = list()

        def at_instrument_stp(s: typing.Union[Instrument.Variable, Instrument.State]):
            self._instrument_stp_variables.append(s)
            s.data.use_standard_pressure = True
            s.data.use_standard_temperature = True
            return s

        self.instrument_report = self.report(
            at_instrument_stp(self.variable_total_scattering(self.data_Bs, self.dimension_wavelength, code="Bs")),
            at_instrument_stp(self.variable_back_scattering(self.data_Bbs, self.dimension_wavelength, code="Bbs")),

            self.variable_air_pressure(self.data_Psample, "sample_pressure", code="P",
                                       attributes={'long_name': "measurement cell pressure"}),
            self.variable_air_temperature(self.data_Tsample, "sample_temperature", code="T",
                                          attributes={'long_name': "measurement cell temperature"}),
            self.variable_temperature(self.data_Tcell, "cell_temperature", code="Tx",
                                      attributes={'long_name': "cell enclosure temperature"}),
            self.variable_air_rh(self.data_Usample, "sample_humidity", code="U",
                                 attributes={'long_name': "measurement cell relative humidity"}),

            self.variable(self.data_Cd, "dark_counts", code="Cd", attributes={
                'long_name': "dark count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),
            self.variable_array(self.data_Cs, self.dimension_wavelength, "scattering_counts", code="Cs", attributes={
                'long_name': "total scattering photon count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),
            self.variable_array(self.data_Cbs, self.dimension_wavelength, "backscattering_counts", code="Cbs", attributes={
                'long_name': "backwards hemispheric scattering photon count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),
            self.variable_array(self.data_Cf, self.dimension_wavelength, "reference_counts", code="Cf", attributes={
                'long_name': "reference shutter photon count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),

            flags=[
                self.flag_bit(self.bit_flags, 0x0001, "backscatter_fault", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0002, "backscatter_digital_fault", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0004, "shutter_fault", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0008, "light_source_fault", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0010, "pressure_sensor_fault", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0020, "enclosure_temperature_fault", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0040, "sample_temperature_fault", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0080, "rh_fault", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0100, "pmt_fault", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0200, "warmup_fault", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0400, "backscatter_high_warning", is_warning=True),
                self.flag_bit(self.bit_flags, 0x8000, "system_fault", is_warning=True),
                self.flag(self.notify_blank, 0x4000),
                self.flag(self.notify_zero, 0x2000),
                self.flag(self.notify_spancheck),
                self.flag(self.notify_calibration),
                self.flag(self.notify_inconsistent_zero),
            ],

            auxiliary_variables=(
                [self.variable(s) for s in self.data_Bs_wavelength] +
                [self.variable(s) for s in self.data_Bbs_wavelength] +
                [self.variable(s) for s in self.data_Cs_wavelength] +
                [self.variable(s) for s in self.data_Cbs_wavelength] +
                [self.variable(s) for s in self.data_Cf_wavelength]
            )
        )
        self.polar_report: typing.Optional[Report] = None

        self.instrument_state = self.change_event(
            self.state_enum(self.data_sampling, attributes={
                'long_name': "sampling mode",
            }),
            self.state_enum(self.data_major_state, attributes={
                'long_name': "major state",
            }),
        )

        self.zero_state = self.change_event(
            self.state_temperature(self.data_Tzero, "zero_temperature", code="Tw", attributes={
                'long_name': "measurement cell temperature during the zero"
            }),
            self.state_pressure(self.data_Pzero, "zero_pressure", code="Pw", attributes={
                'long_name': "measurement cell pressure during the zero"
            }),
            at_instrument_stp(self.state_wall_total_scattering(self.data_Bsw, self.dimension_wavelength, code="Bsw")),
            at_instrument_stp(self.state_wall_back_scattering(self.data_Bbsw, self.dimension_wavelength, code="Bbsw")),
            name="zero",
        )
        self.polar_zero_state: typing.Optional[ChangeEvent] = None

        self.spancheck_state = self.change_event(
            self.state_measurement_array(
                self.data_PCTc, self.dimension_wavelength, "scattering_percent_error", code="PCTc", attributes={
                    'long_name': "spancheck total light scattering percent error",
                    'units': "%",
                    'C_format': "%6.2f"
                }).at_stp(),
            self.state_measurement_array(
                self.data_PCTbc, self.dimension_wavelength, "backscattering_percent_error", code="PCTbc", attributes={
                    'long_name': "spancheck backwards hemispheric light scattering percent error",
                    'units': "%",
                    'C_format': "%6.2f"
                }).at_stp(),
            self.state_measurement_array(
                self.data_Cc, self.dimension_wavelength, "scattering_sensitivity_factor", code="Cc", attributes={
                    'long_name': "total photon count rate attributable to Rayleigh scattering by air at STP",
                    'units': "Hz",
                    'C_format': "%7.1f"
                }).at_stp(),
            self.state_measurement_array(
                self.data_Cbc, self.dimension_wavelength, "backscattering_sensitivity_factor", code="Cbc", attributes={
                    'long_name': "backwards hemispheric photon count rate attributable to Rayleigh scattering by air at STP",
                    'units': "Hz",
                    'C_format': "%7.1f"
                }).at_stp(),
            name="spancheck",
        )
        self.polar_spancheck_state: typing.Optional[ChangeEvent] = None

        self._zero_request: bool = False
        self.context.bus.connect_command('start_zero', self.start_zero)
        self._reboot_request: bool = False
        self.context.bus.connect_command('reboot', self.reboot)

        self.parameters_record = self.context.data.constant_record("parameters")
        self.parameter_ee = self.parameters_record.string("instrument_parameters", attributes={
            'long_name': "instrument response to the EE command",
        })

    def _declare_polar(self) -> None:
        if self.polar_report:
            return

        self.dimension_angle = self.dimension(self.data_angle, "angle", code="Bn", attributes={
            'long_name': "polar scattering start angle (zero is total scattering)",
            'units': "degrees",
            'C_format': "%2.0f"
        })

        var = self.variable_array(self.data_Bsn, [self.dimension_angle, self.dimension_wavelength],
                                  "polar_scattering_coefficient", code="Bsn", attributes={
                'long_name': "polar light scattering coefficient",
                'units': "Mm-1",
                'C_format': "%7.2f"
            })
        self._instrument_stp_variables.append(var)
        var.data.use_standard_pressure = self._instrument_stp_variables[0].data.use_standard_pressure
        var.data.use_standard_temperature = self._instrument_stp_variables[0].data.use_standard_temperature
        self.polar_report = self.report(var)

        var = self.state_measurement_array(self.data_Bsnw, [self.dimension_angle, self.dimension_wavelength],
                                           "polar_wall_scattering_coefficient", code="Bsnw", attributes={
                'long_name': "polar light scattering coefficient from wall signal",
                'units': "Mm-1",
                'C_format': "%7.2f"
            })
        self._instrument_stp_variables.append(var)
        var.data.use_standard_pressure = self._instrument_stp_variables[0].data.use_standard_pressure
        var.data.use_standard_temperature = self._instrument_stp_variables[0].data.use_standard_temperature
        self.polar_zero_state = self.change_event(
            self.state_temperature(self.data_Tzero, "zero_temperature", attributes={
                'long_name': "measurement cell temperature during the zero"
            }),
            self.state_pressure(self.data_Pzero, "zero_pressure", attributes={
                'long_name': "measurement cell pressure during the zero"
            }),
            var,
            name="polar_zero")
        self.polar_zero_state.data_record.standard_temperature = self.zero_state.data_record.standard_temperature
        self.polar_zero_state.data_record.standard_pressure = self.zero_state.data_record.standard_pressure

        self.polar_spancheck_state = self.change_event(
            self.state_measurement_array(self.data_PCTnc, [self.dimension_angle, self.dimension_wavelength],
                                         "polar_scattering_percent_error", code="PCTnc", attributes={
                    'long_name': "spancheck polar light scattering percent error",
                    'units': "%",
                    'C_format': "%6.2f"
                }).at_stp(),
            name="polar_spancheck"
        )

    def _find_angle(self, target: float) -> typing.Optional[int]:
        for angle in range(len(self.data_angle.value)):
            if abs(self.data_angle.value[angle] - target) < 5.0:
                return angle
        return None

    @property
    def _have_backscatter(self) -> bool:
        if self._enable_backscatter is not None:
            return bool(self._enable_backscatter)
        return self._back_scattering_index is not None

    @property
    def _scatterings_valid(self) -> bool:
        if self.data_sampling.value != self.SamplingMode.Normal:
            return False
        if self.data_major_state.value != self.MajorState.Normal:
            return False
        if bool(self.notify_blank):
            return False
        if bool(self.notify_zero):
            return False
        return True

    @property
    def _is_polar(self) -> bool:
        if self._polar_mode is not None:
            return bool(self._polar_mode)
        return bool(self._instrument_polar)

    @property
    def _total_scattering_index(self) -> typing.Optional[int]:
        if not self._is_polar:
            return 0
        return self._find_angle(0.0)

    @property
    def _total_scattering_angle(self) -> typing.Optional[float]:
        angle = self._total_scattering_index
        if angle is None:
            return None
        return self.data_angle.value[angle]

    @property
    def _back_scattering_index(self) -> typing.Optional[int]:
        if self._enable_backscatter is not None:
            if not bool(self._enable_backscatter):
                return None
        if not self._is_polar:
            if len(self.data_angle.value) < 2:
                return None
            return -1
        return self._find_angle(90.0)

    @property
    def _back_scattering_angle(self) -> typing.Optional[float]:
        angle = self._back_scattering_index
        if angle is None:
            return None
        return self.data_angle.value[angle]

    def _process_data_line(self, line: bytes) -> None:
        fields = line.split(b',')
        try:
            (
                date_time,
                *Bsn,
                Tsample, Tcell, Usample, Psample,
                major_state, dio_state
            ) = fields
            if len(Bsn) == 0:
                raise ValueError
            if len(Bsn) % 3 != 0:
                raise ValueError
        except ValueError:
            raise CommunicationsError(f"invalid number of fields in {line}")

        fields = date_time.strip().split(b' ')
        try:
            (
                _,  # Date, because the instrument can use D/M/Y or M/D/Y, so we can't tell which is active
                instrument_time
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid instrument date in {line}")

        parse_time(instrument_time)
        try:
            major_state = self.MajorState(int(major_state))
        except ValueError:
            raise CommunicationsError(f"invalid major state {major_state} date in {line}")
        try:
            dio_state = int(dio_state, 16)
        except ValueError:
            raise CommunicationsError(f"invalid DIO state {dio_state} date in {line}")

        self.data_major_state(major_state)
        Tsample = self.data_Tsample(parse_number(Tsample))
        self.data_Tcell(parse_number(Tcell))
        self.data_Usample(parse_number(Usample))
        Psample = self.data_Psample(parse_number(Psample))

        for i in range(len(Bsn)):
            Bsn[i] = parse_number(Bsn[i])
            if Bsn[i] == -9999:
                Bsn[i] = nan

        # Can happen with disabled angles
        while len(Bsn) > 3 and not isfinite(Bsn[-1]) and not isfinite(Bsn[-2]) and not isfinite(Bsn[-3]):
            del Bsn[-3:]

        # Transform to angle major and B, G, R
        Bsn = [
            [Bsn[angle * 3 + 2], Bsn[angle * 3 + 1], Bsn[angle * 3 + 0]]
            for angle in range(len(Bsn)//3)
        ]

        if self._control_zero_state == self._ControlZeroState.MeasureZero:
            self.average_Tzero(Tsample)
            self.average_Pzero(Psample)
            for angle in range(len(Bsn)):
                while angle >= len(self.average_Bszero):
                    self.average_Bszero.append([
                        self._ZeroAverage(),
                        self._ZeroAverage(),
                        self._ZeroAverage(),
                    ])
                for wavelength in range(3):
                    self.average_Bszero[angle][wavelength](Bsn[angle][wavelength])
        elif self._zero_mode == self._ZeroMode.Offset:
            zero_offsets = self.data_Bsnw_offset.value
            if zero_offsets and len(zero_offsets) == len(Bsn):
                for angle in range(len(Bsn)):
                    for wavelength in range(3):
                        offset = zero_offsets[angle][wavelength]
                        if offset is not None and isfinite(offset):
                            Bsn[angle][wavelength] -= offset

        if len(Bsn) == 1:
            if self._instrument_polar is None:
                self._instrument_polar = False
        elif len(Bsn) == 2:
            if self._instrument_polar is None:
                self._instrument_polar = False
        else:
            if self._instrument_polar is None:
                self._instrument_polar = True

        angle = self._total_scattering_index
        if angle is not None:
            for wavelength in range(len(self.data_Bs_wavelength)):
                self.data_Bs_wavelength[wavelength](Bsn[angle][wavelength])
        angle = self._back_scattering_index
        if angle < 0 and len(Bsn) == 1:
            angle = None
        if angle is not None:
            for wavelength in range(len(self.data_Bbs_wavelength)):
                self.data_Bbs_wavelength[wavelength](Bsn[angle][wavelength])

        for wavelength in range(len(self.data_Bsn_wavelength)):
            self.data_Bsn_wavelength[wavelength]([Bsn[angle][wavelength] for angle in range(len(Bsn))])

        spancheck_phase: typing.Optional[Instrument._Spancheck.Phase] = self._spancheck.active_phase
        if spancheck_phase:
            spancheck_phase.temperature(Tsample)
            spancheck_phase.pressure(Psample)
            for wavelength in range(len(self.data_wavelength.value)):
                for angle in range(len(self.data_angle.value)):
                    try:
                        wla = spancheck_phase[self.data_wavelength.value[wavelength]][self.data_angle.value[angle]]
                    except IndexError:
                        continue
                    wla.scattering(Bsn[angle][wavelength])

    @staticmethod
    def _calculate_zero_change(prior: float, current: float) -> float:
        if prior is None or current is None:
            return nan
        if not isfinite(prior) or not isfinite(current):
            return nan
        return current - prior

    def _process_zero(self, Bsnw: typing.List[typing.List[float]],
                      zero_temperature: float, zero_pressure: float) -> None:
        if self._is_polar:
            self._declare_polar()

        self.data_Tzero(zero_temperature, oneshot=True)
        self.data_Pzero(zero_pressure, oneshot=True)

        if Bsnw:
            for wavelength in range(len(self.data_Bsnw_wavelength)):
                self.data_Bsnw_wavelength[wavelength]([angle[wavelength] for angle in Bsnw], oneshot=True)
            Bsnw = self.data_Bsnw([
                [c.value[angle] for c in self.data_Bsnw_wavelength] for angle in range(len(Bsnw))
            ], oneshot=True)

        angle = self._total_scattering_index
        if angle is not None and angle < len(Bsnw):
            for wavelength in range(len(self.WAVELENGTHS)):
                prior = self.data_Bsw_wavelength[wavelength].value
                current = self.data_Bsw_wavelength[wavelength](Bsnw[angle][wavelength], oneshot=True)
                self.data_Bswd_wavelength[wavelength](self._calculate_zero_change(prior, current))
            self.data_Bsw([c.value for c in self.data_Bsw_wavelength], oneshot=True)

        angle = self._back_scattering_index
        if angle is not None and angle < len(Bsnw):
            for wavelength in range(len(self.WAVELENGTHS)):
                prior = self.data_Bbsw_wavelength[wavelength].value
                current = self.data_Bbsw_wavelength[wavelength](Bsnw[angle][wavelength], oneshot=True)
                self.data_Bbswd_wavelength[wavelength](self._calculate_zero_change(prior, current))
            self.data_Bbsw([c.value for c in self.data_Bbsw_wavelength], oneshot=True)

    async def _ee(self, force_zero_update: bool = False) -> None:
        self.writer.write(b"EE\r")
        lines = await self.read_multiple_lines(total=10.0, first=2.0, tail=1.0)

        self.parameter_ee("\n".join([
            l.decode('utf-8', errors='backslashreplace') for l in lines
        ]))

        self._parameter_change_pending = False
        self._instrument_zero_pending = False

        parameters: typing.Dict[bytes, bytes] = dict()
        for l in lines:
            try:
                (key, value) = l.split(b'=', 1)
            except ValueError:
                continue
            key = _EE_SPACE_REMOVE.sub(b' ', key)
            key = key.strip().lower()
            if not key:
                continue
            value = value.strip()
            if value.startswith(b','):
                value = value[1:].strip()
            if not value:
                continue

            parameters[key] = value

        sn = parameters.get(b"serial number")
        if sn:
            self.set_serial_number(sn)

        fw_major = parameters.get(b"version major")
        fw_minor = parameters.get(b"version minor")
        fw_rev = parameters.get(b"version revision")
        if fw_major and fw_minor and fw_rev:
            self.set_firmware_version(fw_major + b'.' + fw_minor + b'.' + fw_rev)

        temperature_unit = parameters.get(b'temperature unit')
        if temperature_unit and not temperature_unit.lower().endswith(b'c'):
            raise CommunicationsError(f"invalid temperature unit {temperature_unit}")

        pressure_unit = parameters.get(b"atmpressureunit")
        if pressure_unit:
            pressure_unit = pressure_unit.lower()
            if pressure_unit != b"mb" and pressure_unit != b"mbar" and pressure_unit != b"hpa":
                raise CommunicationsError(f"invalid pressure unit {pressure_unit}")

        stp_t = parameters.get(b"normalise to")
        if not stp_t:
            stp_t = parameters.get(b"normalize to")
        if stp_t:
            if stp_t.lower() != b"none":
                while stp_t and not stp_t[-1:].isdigit():
                    stp_t = stp_t[:-1]
                try:
                    stp_t = float(stp_t)
                    if stp_t > 150.0:
                        stp_t -= 273.15
                except ValueError:
                    stp_t = None
            else:
                stp_t = None
            if stp_t is not None:
                for var in self._instrument_stp_variables:
                    var.data.use_standard_pressure = True
                    var.data.use_standard_temperature = True
                self.instrument_report.record.data_record.standard_temperature = stp_t
                self.instrument_report.record.data_record.standard_pressure = ONE_ATM_IN_HPA
                self.zero_state.data_record.standard_temperature = stp_t
                self.zero_state.data_record.standard_pressure = ONE_ATM_IN_HPA
                if self.polar_zero_state:
                    self.polar_zero_state.data_record.standard_temperature = stp_t
                    self.polar_zero_state.data_record.standard_pressure = ONE_ATM_IN_HPA
            else:
                for var in self._instrument_stp_variables:
                    var.data.use_standard_pressure = True
                    var.data.use_standard_temperature = True
                self.instrument_report.record.data_record.standard_temperature = None
                self.instrument_report.record.data_record.standard_pressure = None
                self.zero_state.data_record.standard_temperature = None
                self.zero_state.data_record.standard_pressure = None
                if self.polar_zero_state:
                    self.polar_zero_state.data_record.standard_temperature = None
                    self.polar_zero_state.data_record.standard_pressure = None

        filter_mode = parameters.get(b"filtering method")
        if filter_mode:
            filter_mode = filter_mode[:1].lower()
            instrument_filter_active = (filter_mode in (b"k", b"m", b"a"))
            if self._can_set_filter_mode:
                self.notify_inconsistent_zero(False)
            else:
                if self._zero_mode == self._ZeroMode.Offset:
                    self.notify_inconsistent_zero(instrument_filter_active)
                else:
                    self.notify_inconsistent_zero(not instrument_filter_active)

        def get_number_suffix(key: bytes) -> float:
            try:
                value = parameters[key]
            except KeyError:
                raise CommunicationsError(f"{key} not found in EE record")
            while value and not value[-1:].isdigit():
                value = value[:-1]
            try:
                return float(value)
            except ValueError:
                raise CommunicationsError(f"EE {key} has invalid value {value}")

        self.data_wavelength([
            get_number_suffix(b"wavelength 3"),
            get_number_suffix(b"wavelength 2"),
            get_number_suffix(b"wavelength 1"),
        ])

        def get_array(key: bytes) -> typing.Optional[typing.List[float]]:
            try:
                value = parameters[key]
            except KeyError:
                return None
            result: typing.List[float] = list()
            for v in value.split(b','):
                v = v.strip()
                try:
                    v = float(v)
                    if not isfinite(v):
                        raise ValueError
                except ValueError:
                    raise CommunicationsError(f"EE {key} has invalid value {value}")
                result.append(v)
            return result

        angles = get_array(b"angle list")
        if not angles:
            if self._polar_mode:
                angles = [0, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90]
            elif self._enable_backscatter is not None and not bool(self._enable_backscatter):
                angles = [0.0]
            else:
                angles = [0, 90]
        else:
            if len(angles) > 2:
                self._instrument_polar = True
            elif abs(angles[0]) >= 5.0:
                self._instrument_polar = True
            elif len(angles) == 2 and abs(angles[-1] - 90) >= 5.0:
                self._instrument_polar = True
            else:
                self._instrument_polar = False
        self.data_angle(angles)

        def get_wavelength_array(base: bytes) -> typing.Optional[typing.List[typing.List[float]]]:
            result: typing.List[typing.List[float]] = list()
            for angle in range(len(angles)):
                result.append([nan] * len(self.WAVELENGTHS))

            for wavelength in range(len(self.WAVELENGTHS)):
                angle_data = get_array(base + b" %d" % (len(self.WAVELENGTHS) - wavelength))
                if not angle_data:
                    return None
                if len(angle_data) != len(angles):
                    raise CommunicationsError(f"invalid wavelength data {base} {wavelength}")
                for angle in range(len(angles)):
                    result[angle][wavelength] = angle_data[angle]
            return result

        cal_m = get_wavelength_array(b"calibration ms")
        cal_c = get_wavelength_array(b"calibration cs")
        zero_adj_x = get_wavelength_array(b"cal zeroadj xs")
        cal_wall = get_wavelength_array(b"calibration walls")

        Bsnw: typing.List[typing.List[float]] = list()
        for angle in range(len(angles)):
            Bsnw.append([nan] * len(self.WAVELENGTHS))
        if cal_m and cal_c:
            for angle in range(len(angles)):
                for wavelength in range(len(self.WAVELENGTHS)):
                    M = cal_m[angle][wavelength]
                    C = cal_c[angle][wavelength]
                    if abs(M) < 1E-9:
                        continue
                    Bsnw[angle][wavelength] = C / M
        if cal_wall and zero_adj_x:
            for angle in range(len(angles)):
                for wavelength in range(len(self.WAVELENGTHS)):
                    if isfinite(Bsnw[angle][wavelength]):
                        continue
                    rayleigh = zero_adj_x[angle][wavelength]
                    wall = cal_wall[angle][wavelength]
                    if wall <= 1.0 or wall >= 100.0:
                        continue
                    wall /= 100.0
                    Bsnw[angle][wavelength] = (wall * rayleigh) / (1.0 - wall)

        def instrument_zero_changed() -> bool:
            if force_zero_update:
                return True
            prior = self.data_Bsnw_base.value
            if not prior:
                return True
            if len(prior) != len(Bsnw):
                return True
            for angle in range(len(Bsnw)):
                for wavelength in range(len(self.WAVELENGTHS)):
                    if not isfinite(Bsnw[angle][wavelength]):
                        if isfinite(prior[angle][wavelength]):
                            return True
                        continue
                    if not isfinite(prior[angle][wavelength]):
                        return True
                    if abs(Bsnw[angle][wavelength] - prior[angle][wavelength]) > 0.1:
                        return True
            return False

        zero_updated = instrument_zero_changed()

        zero_temperature = parameters.get(b'cal zeroadj temp')
        if zero_temperature is not None:
            zero_temperature = float(zero_temperature)
            if zero_temperature > 150.0:
                zero_temperature -= 273.15
        zero_pressure = parameters.get(b'cal zeroadj pressure')
        if zero_pressure is not None:
            zero_pressure = float(zero_pressure)

        if zero_updated:
            if zero_temperature is None or zero_pressure is None:
                raise CommunicationsError("zero temperature or pressure missing from EE")

            _LOGGER.debug("Processing zero update")
            self.data_Bsnw_offset(None, oneshot=True)
            self.data_Bsnw_base(Bsnw, oneshot=True)
            self._process_zero(Bsnw, zero_temperature, zero_pressure)

    async def _read_response(self) -> bytes:
        line = bytearray()
        while len(line) < 1024:
            d = await self.reader.read(1)
            if not d:
                break
            if d == b'\r' or d == b'\n':
                line = line.strip()
                if line:
                    break
                line.clear()
                continue
            if d == b'\x15' or d == b'\x00':
                raise InstrumentBusy
            line += d
        return bytes(line)

    async def _vi_command(self, vi: "Instrument._VI") -> bytes:
        await self._command_delay()
        self.writer.write(b"VI%d%02d\r" % (self._address, int(vi)))
        return await wait_cancelable(self._read_response(), 4.0)

    async def _set_digital_span(self, enable: bool) -> None:
        await self._command_delay()
        self.writer.write(b"DO%d00%d\r" % (self._address, 1 if enable else 0))
        data: bytes = await wait_cancelable(self.read_line(), 4.0)
        if data != b"OK":
            raise CommunicationsError(f"invalid DOSPAN response {data}")

    async def _set_digital_zero(self, enable: bool) -> None:
        await self._command_delay()
        self.writer.write(b"DO%d01%d\r" % (self._address, 1 if enable else 0))
        data: bytes = await wait_cancelable(self.read_line(), 4.0)
        if data != b"OK":
            raise CommunicationsError(f"invalid DOZERO response {data}")

    @staticmethod
    def _parse_system_state(response: bytes) -> typing.Tuple["Instrument.MajorState", typing.Optional[typing.Union["Instrument._NormalMinorState"]]]:
        if len(response) < 4 or len(response) > 10:
            raise CommunicationsError(f"invalid system state {response}")
        try:
            major, minor = response.split(b'.')
            major = int(major)
            minor = int(minor)
            major = Instrument.MajorState(major)
        except ValueError:
            raise CommunicationsError(f"invalid system state {response}")
        if major == Instrument.MajorState.Normal:
            try:
                minor = Instrument._NormalMinorState(minor)
            except ValueError:
                raise CommunicationsError(f"invalid minor state in {response}")
        else:
            minor = None
        return major, minor

    async def start_communications(self) -> None:
        self._instrument_polar = None
        self._zero_request = None
        self._spancheck.abort_desynchronized()

        if self._zero_schedule:
            discard_zero = self._zero_schedule.current()
            discard_zero.activate()

        if self.writer:
            await self.drain_reader(2.0)

            self.writer.write(b"ID%d\r" % self._address)
            instrument_id = await wait_cancelable(self.read_line(), 4.0)
            matched = _INSTRUMENT_ID.search(instrument_id)
            if matched:
                model = matched.group(1)
                self.set_firmware_version(matched.group(2))
                self.set_serial_number(matched.group(3))

                if self._instrument_polar is None:
                    if model == b"4000":
                        self._instrument_polar = True
                    elif model == b"3000":
                        self._instrument_polar = False
            elif b"3000" in instrument_id:
                if self._instrument_polar is None:
                    self._instrument_polar = False
            elif b"4000" in instrument_id:
                if self._instrument_polar is None:
                    self._instrument_polar = True

            system_state = await self._vi_command(self._VI.SystemState)
            self._parse_system_state(system_state)

            await self._command_delay()
            ts = time.gmtime()
            self.writer.write(f"**{self._address}S"
                              f"{ts.tm_hour:02}{ts.tm_min:02}{ts.tm_sec:02}"
                              f"{ts.tm_mday:02}{ts.tm_mon:02}{ts.tm_year%100:02}\r".encode('ascii'))
            await self.writer.drain()
            data: bytes = await wait_cancelable(self.read_line(), 4.0)
            if data != b"OK":
                raise CommunicationsError(f"set time response: {data}")

            await self._command_delay()
            filter_mode = "N"
            if self._zero_mode == self._ZeroMode.Native:
                filter_mode = self._filter_mode.command_code
            self.writer.write(f"**{self._address}PCF,{filter_mode}\n\r".encode('ascii'))
            await self.writer.drain()
            try:
                data: bytes = await wait_cancelable(self.read_line(), 4.0)
                if data != b"OK":
                    raise ValueError
                self._can_set_filter_mode = True
            except (TimeoutError, asyncio.TimeoutError, ValueError):
                _LOGGER.debug("Filter control unavailable")
                self._can_set_filter_mode = False

            if self._can_set_filter_mode:
                self.writer.write(f"**{self._address}PCSTP,0\n\r".encode('ascii'))
                await self.writer.drain()
                try:
                    data: bytes = await wait_cancelable(self.read_line(), 4.0)
                    if data != b"OK":
                        raise ValueError
                    self._can_set_filter_mode = True
                except (TimeoutError, asyncio.TimeoutError, ValueError):
                    _LOGGER.debug("Unable to set STP temperature")
                    self._can_set_filter_mode = False

            await self._command_delay()
            await self._ee()

            try:
                data: bytes = await wait_cancelable(self._vi_command(self._VI.SystemFlags), 4.0)
                if data.endswith(b'.'):
                    data = data[:-1]
                int(data)
                self._have_system_flags = True
            except (TimeoutError, asyncio.TimeoutError, ValueError):
                _LOGGER.debug("System flags unavailable")
                self._have_system_flags = False

            await self._set_digital_span(False)
            await self._set_digital_zero(False)

        await self.drain_reader(0.5)
        if self.writer:
            await self._command_delay()
            self.writer.write(b"VI%d99\r" % self._address)
            await self.writer.drain()
            await wait_cancelable(self._read_response(), 2.0)

            line: bytes = await wait_cancelable(self._vi_command(self._VI.DataLine), 4.0)
            self._process_data_line(line)
        else:
            await wait_cancelable(self.read_line(), self._instrument_update_time * 2.0 + 1.0)
            line: bytes = await wait_cancelable(self.read_line(), self._instrument_update_time * 2.0 + 5.0)
            self._process_data_line(line)

        self._sleep_time = 0.0

    def _complete_offset_zero(self) -> None:
        Tzero = self.average_Tzero.mean()
        Pzero = self.average_Pzero.mean()
        self.average_Tzero.reset()
        self.average_Pzero.reset()

        Bsnw_offset = self.data_Bsnw_offset([
            [c.mean() for c in angle] for angle in self.average_Bszero
        ], oneshot=True)
        self.average_Bszero.clear()
        Bsnw_base = self.data_Bsnw_base.value
        if not Bsnw_base or len(Bsnw_offset) != len(Bsnw_base):
            self._process_zero([], Tzero, Pzero)
            return

        Bsnw = [
            [(Bsnw_base[angle][wavelength] + Bsnw_offset[angle][wavelength])
             for wavelength in range(len(self.WAVELENGTHS))] for angle in range(len(Bsnw_offset))
        ]
        self._process_zero(Bsnw, Tzero, Pzero)

    async def _update_state(self) -> None:
        if self._zero_schedule:
            now = time.time()
            zero = self._zero_schedule.current(now)
            if zero.activate(now):
                if self.data_sampling.value != self.SamplingMode.Normal:
                    _LOGGER.debug("Ignoring scheduled zero while not in normal operating mode")
                else:
                    _LOGGER.debug("Automatic zero scheduled")
                    self._zero_request = True

        if self._spancheck.is_running:
            if self._zero_request:
                _LOGGER.debug("Discarded queued zero due to active spancheck")
                self._zero_request = False
            self._control_zero_state = self._ControlZeroState.Idle
            return

        if self.data_major_state.value != self.MajorState.Normal:
            # Force a flush whenever the instrument exits back to normal mode
            self._control_zero_state = self._ControlZeroState.FlushAir
            self._control_zero_advance = time.monotonic() + self._air_flush_time
        elif self._control_zero_state == self._ControlZeroState.FillZero:
            if time.monotonic() > self._control_zero_advance:
                _LOGGER.debug("Starting zero measurement")
                self._control_zero_state = self._ControlZeroState.MeasureZero
                self._control_zero_advance = time.monotonic() + self._zero_measure_time
                self.average_Bszero.clear()
                self.average_Tzero.reset()
                self.average_Pzero.reset()
        elif self._control_zero_state == self._ControlZeroState.MeasureZero:
            if time.monotonic() > self._control_zero_advance and self.average_Pzero.count > 0:
                if self.writer:
                    await self._set_digital_zero(False)
                self._complete_offset_zero()
                _LOGGER.debug("Zero measurement completed")
                self._control_zero_state = self._ControlZeroState.FlushAir
                self._control_zero_advance = time.monotonic() + self._air_flush_time
        elif self._control_zero_state == self._ControlZeroState.FlushAir:
            if time.monotonic() > self._control_zero_advance:
                self._control_zero_state = self._ControlZeroState.Idle

        if self._filter_mode_reset_needed is not None:
            if self.data_major_state.value == self.MajorState.Normal:
                self._filter_mode_reset_needed -= 1
                if self._filter_mode_reset_needed <= 0:
                    self._filter_mode_reset_needed = None
                    if self.writer and self._can_set_filter_mode:
                        self.writer.write(f"**{self._address}PCF,{self._FilterMode.Disabled.command_code}\n\r".encode('ascii'))
                        data: bytes = await wait_cancelable(self.read_line(), 4.0)
                        if data != b"OK":
                            raise CommunicationsError(f"invalid PCF response {data}")
                    else:
                        _LOGGER.warning("Unable to reset filter mode after zero on a read only instrument")
            else:
                self._filter_mode_reset_needed = 3

        if self._zero_request:
            self._zero_request = False
            if not self.writer:
                _LOGGER.warning("Unable to start zero on a read only instrument")
            else:
                if self._zero_mode == self._ZeroMode.Offset:
                    _LOGGER.debug("Starting zero air fill")
                    self._control_zero_state = self._ControlZeroState.FillZero
                    self._control_zero_advance = time.monotonic() + self._zero_fill_time
                    await self._set_digital_zero(True)
                else:
                    if self._zero_mode == self._ZeroMode.SetFilter:
                        if self._can_set_filter_mode:
                            self._filter_mode_reset_needed = 3
                            self.writer.write(f"**{self._address}PCF,{self._filter_mode.command_code}\n\r".encode('ascii'))
                            data: bytes = await wait_cancelable(self.read_line(), 4.0)
                            if data != b"OK":
                                raise CommunicationsError(f"invalid PCF response {data}")
                        else:
                            _LOGGER.warning("Unable to change filter mode for zeroing")

                    self.writer.write(f"**{self._address}J5\r".encode('ascii'))
                    data: bytes = await wait_cancelable(self.read_line(), 4.0)
                    if data != b"OK":
                        raise CommunicationsError(f"invalid J5 response {data}")

        if self._apply_spancheck_calibration_request:
            self._apply_spancheck_calibration_request = False
            if not self.writer:
                _LOGGER.warning("Unable to apply spancheck calibration on a read only instrument")
            elif not self.data_spancheck_result.value:
                _LOGGER.debug("No spancheck results available")
            else:
                def unpack_result(result: typing.Dict[str, typing.Any]) -> typing.Tuple[typing.List[typing.List[float]], typing.List[typing.List[float]]]:
                    calC = result.get('calibration', {}).get('C', {})
                    calM = result.get('calibration', {}).get('M', {})

                    resultC: typing.List[typing.List[float]] = list()
                    resultM: typing.List[typing.List[float]] = list()
                    for angle in self.data_angle.value:
                        check = calC.get(str(int(angle)))
                        if not check:
                            if abs(angle) < 5:
                                check = calC.get('total')
                            elif abs(angle - 90) < 5:
                                check = calC.get('back')
                        if not check:
                            check = {}
                        add = list()
                        for _, code in self.WAVELENGTHS:
                            add.append(check.get(code))
                        resultC.append(add)

                        check = calM.get(str(int(angle)))
                        if not check:
                            if abs(angle) < 5:
                                check = calM.get('total')
                            elif abs(angle - 90) < 5:
                                check = calM.get('back')
                        if not check:
                            check = {}
                        add = list()
                        for _, code in self.WAVELENGTHS:
                            add.append(check.get(code))
                        resultM.append(add)
                    return resultC, resultM

                def result_commands(result: typing.Dict[str, typing.Any]) -> typing.Optional[typing.List[bytes]]:
                    commands: typing.List[bytes] = list()
                    resultC, resultM = unpack_result(result)
                    for wavelength in range(len(self.WAVELENGTHS)):
                        for angle in range(len(self.data_angle.value)):
                            calM = resultM[angle][wavelength]
                            calC = resultC[angle][wavelength]
                            if calM is None or not isfinite(calM) or calM <= 0.0 or calM >= 0.1:
                                return None
                            if calC is None or not isfinite(calC) or calC <= 0.0 or calC >= 0.1:
                                return None
                            commands.append(f"**{self._address}PCM{3 - wavelength}{angle + 1:02d},{calM:.10f}\n\r".encode('ascii'))
                            commands.append(f"**{self._address}PCC{3 - wavelength}{angle + 1:02d},{calC:.10f}\n\r".encode('ascii'))
                    return commands

                commands = result_commands(self.data_spancheck_result.value)
                if not commands:
                    self.context.bus.log(
                        f"Spancheck calibration is invalid.  No instrument settings changed." + (
                            "  For polar nephelometers, the instrument does not report sufficient data to calculate "
                            "the calibration.  You must use the on-board calibration facility."
                            if self._is_polar else ""
                        ),
                        {
                            'M': self.data_spancheck_result.value.get('calibration', {}).get('M'),
                            'C': self.data_spancheck_result.value.get('calibration', {}).get('C'),
                        },
                        type=BaseBusInterface.LogType.ERROR)
                    _LOGGER.warning(f"Ignoring invalid spancheck calibration application")
                else:
                    for c in commands:
                        self.writer.write(c)
                        data: bytes = await wait_cancelable(self.read_line(), 4.0)
                        if data != b"OK":
                            raise CommunicationsError(f"invalid command {c.strip()} response {data}")
                    self.context.bus.log(f"Spancheck calibration saved to instrument settings", {
                        'M': self.data_spancheck_result.value.get('calibration', {}).get('M'),
                        'C': self.data_spancheck_result.value.get('calibration', {}).get('C'),
                    })
                    _LOGGER.debug("Spancheck calibration applied")

        if self._reboot_request:
            self._reboot_request = None
            if not self.writer:
                _LOGGER.warning("Unable to reboot a read only instrument")
            else:
                _LOGGER.debug("Issuing instrument reboot")
                self.writer.write(f"**{self._address}B\r".encode('ascii'))
                await asyncio.sleep(5.0)
                raise CommunicationsError("instrument rebooting")

    def start_zero(self, _) -> None:
        if self.data_sampling.value != self.SamplingMode.Normal:
            _LOGGER.debug("Discarding zero request while not in normal operation mode")
            return
        self._zero_request = True

    def reboot(self, _) -> None:
        self._reboot_request = True

    def apply_spancheck_calibration(self, _) -> None:
        if self.data_sampling.value != self.SamplingMode.Normal:
            _LOGGER.debug("Discarding spancheck apply request while not in normal operation mode")
            return
        self._apply_spancheck_calibration_request = True

    def _output_data(self) -> None:
        sampling_mode = self.SamplingMode.Normal

        if self._is_polar:
            self._declare_polar()

        if self.data_major_state.value in (self.MajorState.ZeroCalibration,
                                           self.MajorState.ZeroAdjust,
                                           self.MajorState.ZeroCheck):
            sampling_mode = self.SamplingMode.Zero
            self._parameter_change_pending = True
            self.notify_zero(True)
            self.notify_blank(False)
        elif self._control_zero_state == self._ControlZeroState.MeasureZero:
            sampling_mode = self.SamplingMode.Zero
            self.notify_zero(True)
            self.notify_blank(False)
        elif self._control_zero_state in (self._ControlZeroState.FillZero, self._ControlZeroState.FlushAir):
            sampling_mode = self.SamplingMode.Blank
            self.notify_zero(False)
            self.notify_blank(True)
        else:
            self.notify_zero(False)
            self.notify_blank(False)
        if self.data_major_state.value == self.MajorState.ZeroAdjust:
            self._instrument_zero_pending = True

        if self.data_major_state.value in (self.MajorState.SpanCalibration, self.MajorState.SpanCheck):
            sampling_mode = self.SamplingMode.Spancheck
            self._parameter_change_pending = True
            self.notify_spancheck(True)
        elif self._spancheck.is_running:
            sampling_mode = self.SamplingMode.Spancheck
            self.notify_spancheck(True)
        else:
            self.notify_spancheck(False)

        if self.data_major_state.value in (self.MajorState.SystemCalibration,
                                           self.MajorState.EnvironmentalCalibration):
            self._parameter_change_pending = True
            self.notify_calibration(True)
        else:
            self.notify_calibration(False)

        self.data_sampling(sampling_mode)

        if self._scatterings_valid:
            self.data_Bs([float(c) for c in self.data_Bs_wavelength])
            if self._have_backscatter:
                self.data_Bbs([float(c) for c in self.data_Bbs_wavelength])
            self.data_Bsn([
                [wavelength.value[angle] for wavelength in self.data_Bsn_wavelength]
                for angle in range(len(self.data_angle.value))
            ])
        else:
            self.data_Bs([nan for _ in self.data_Bs_wavelength])
            if self._have_backscatter:
                self.data_Bbs([nan for _ in self.data_Bbs_wavelength])
            self.data_Bsn([
                [nan for _ in self.data_Bsn_wavelength]
                for _ in range(len(self.data_angle.value))
            ])

        self.data_Cf([float(v) for v in self.data_Cf_wavelength])
        self.data_Cs([float(v) for v in self.data_Cs_wavelength])
        self.data_Cr([float(v) for v in self.data_Cr_wavelength])
        if self._have_backscatter:
            self.data_Cbs([float(v) for v in self.data_Cbs_wavelength])
            self.data_Cbr([float(v) for v in self.data_Cbr_wavelength])

        self.instrument_report()
        if self.polar_report:
            self.polar_report()

    async def communicate(self) -> None:
        await self._spancheck()
        await self._update_state()

        if not self.writer:
            line: bytes = await wait_cancelable(self.read_line(), self._instrument_update_time * 2.0 + 1.0)
            self._process_data_line(line)
            self._output_data()
            return

        if self._sleep_time > 0.0:
            await asyncio.sleep(self._sleep_time)
            self._sleep_time = 0.0
        begin_read = time.monotonic()
        busy_final_timeout = begin_read + self._busy_timeout

        async def retryable_vi(vi: "Instrument._VI", parse: typing.Callable[[bytes], typing.Any]) -> typing.Any:
            command_retries = self._command_retry
            while True:
                try:
                    response = await self._vi_command(vi)
                except (TimeoutError, asyncio.TimeoutError):
                    command_retries -= 1
                    if command_retries < 0:
                        raise
                    _LOGGER.debug(f"Retrying timed out command {int(vi)}", exc_info=True)
                    await asyncio.sleep(2.0)
                    continue

                try:
                    return parse(response)
                except CommunicationsError:
                    command_retries -= 1
                    if command_retries < 0:
                        raise
                    _LOGGER.debug(f"Retrying command {int(vi)}", exc_info=True)
                    await asyncio.sleep(2.0)
                    continue

        was_busy = False
        while True:
            try:
                _, minor = await retryable_vi(
                    self._VI.SystemState,
                    lambda system_state: self._parse_system_state(system_state)
                )
                shutter_final_timeout = time.monotonic() + 10.0
                while minor in (self._NormalMinorState.ShutterUp, self._NormalMinorState.ShutterDown,
                                self._NormalMinorState.ShutterMeasure):
                    if time.monotonic() > shutter_final_timeout:
                        raise CommunicationsError("timeout waiting for shutter measurement to end")
                    _, minor = await retryable_vi(
                        self._VI.SystemState,
                        lambda system_state: self._parse_system_state(system_state)
                    )

                await retryable_vi(
                    self._VI.DataLine,
                    lambda line: self._process_data_line(line)
                )

                spancheck_phase: typing.Optional[Instrument._Spancheck.Phase] = self._spancheck.active_phase

                Cd = self.data_Cd(await retryable_vi(
                    self._VI.DarkCounts, lambda value: _parse_number_limit(
                        value, minimum_acceptable=-100, maximum_acceptable=1E6,
                    )
                ))
                if spancheck_phase:
                    spancheck_phase.dark_counts(Cd)

                for wavelength in range(len(self.WAVELENGTHS)):
                    Cf = self.data_Cf_wavelength[wavelength](await retryable_vi(
                        self._VI.Cf(wavelength), lambda value: _parse_number_limit(
                            value, minimum_acceptable=-100, maximum_acceptable=100E6,
                        )
                    ))

                    Cs = self.data_Cs_wavelength[wavelength](await retryable_vi(
                        self._VI.Cs(wavelength), lambda value: _parse_number_limit(
                            value, minimum_acceptable=-100, maximum_acceptable=1E6,
                        )
                    ))
                    self.data_Cr_wavelength[wavelength](await retryable_vi(
                        self._VI.Cr(wavelength), lambda value: _parse_number_limit(
                            value, minimum_acceptable=-1, maximum_acceptable=100,
                        )
                    ))

                    if spancheck_phase:
                        try:
                            spancheck_phase[self.data_wavelength.value[wavelength]].reference_counts(Cf)
                        except IndexError:
                            pass

                        angle = self._total_scattering_angle
                        if angle is not None:
                            try:
                                wla = spancheck_phase[self.data_wavelength.value[wavelength]][angle]
                                wla.measurement_counts(Cs)
                            except IndexError:
                                pass

                    if self._have_backscatter:
                        Cbs = self.data_Cbs_wavelength[wavelength](await retryable_vi(
                            self._VI.Cbs(wavelength), lambda value: _parse_number_limit(
                                value, minimum_acceptable=-100, maximum_acceptable=1E6,
                            )
                        ))
                        self.data_Cbr_wavelength[wavelength](await retryable_vi(
                            self._VI.Cbr(wavelength), lambda value: _parse_number_limit(
                                value, minimum_acceptable=-1, maximum_acceptable=100,
                            )
                        ))

                        if spancheck_phase:
                            angle = self._back_scattering_angle
                            if angle is not None:
                                try:
                                    wla = spancheck_phase[self.data_wavelength.value[wavelength]][angle]
                                    wla.measurement_counts(Cbs)
                                except IndexError:
                                    pass

                if self._have_system_flags:
                    def parse(flags: bytes):
                        if flags.endswith(b"."):
                            flags = flags[:-1]
                        parse_flags_bits(flags, self.bit_flags)

                    await retryable_vi(self._VI.SystemFlags, parse)
            except InstrumentBusy:
                if busy_final_timeout > time.monotonic():
                    if not was_busy:
                        _LOGGER.debug(f"Instrument busy")
                        was_busy = True

                    await self.drain_reader(2.0)
                    continue
                raise
            break

        if self._parameter_change_pending and self.data_major_state == self.MajorState.Normal:
            await self._ee(self._instrument_zero_pending)

        if self._report_interval:
            self._instrument_update_time = self._report_interval
        else:
            if self._is_polar:
                self._instrument_update_time = len(self.data_angle.value) * 3.0
            else:
                self._instrument_update_time = 3.0

        self._output_data()
        end_read = time.monotonic()
        self._sleep_time = self._instrument_update_time - (end_read - begin_read)
