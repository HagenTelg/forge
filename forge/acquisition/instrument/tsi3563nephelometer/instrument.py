import typing
import asyncio
import time
import enum
import re
import datetime
import serial
import logging
from math import isfinite, nan, floor
from contextlib import asynccontextmanager
from forge.tasks import wait_cancelable
from forge.dewpoint import extrapolate_rh
from forge.units import temperature_k_to_c, ONE_ATM_IN_HPA
from forge.acquisition import LayeredConfiguration
from forge.acquisition.schedule import Schedule
from ..streaming import StreamingInstrument, StreamingContext, CommunicationsError, BaseBusInterface
from ..parse import parse_number, parse_flags_bits
from ..state import Persistent
from ..variable import Input
from ..spancheck import Spancheck as SpancheckController
from .parameters import Parameters

_LOGGER = logging.getLogger(__name__)
_INSTRUMENT_TYPE = __name__.split('.')[-2]
_FIELD_SPLIT = re.compile(rb"(?:\s*,\s*)|\s+")


class Instrument(StreamingInstrument):
    INSTRUMENT_TYPE = _INSTRUMENT_TYPE
    MANUFACTURER = "TSI"
    MODEL = "3563"
    DISPLAY_LETTER = "N"
    TAGS = frozenset({"aerosol", "scattering", _INSTRUMENT_TYPE})
    SERIAL_PORT = {'baudrate': 9600, 'parity': serial.PARITY_EVEN, 'bytesize': serial.SEVENBITS}

    @enum.unique
    class SamplingMode(enum.IntEnum):
        Normal = 0
        Zero = 1
        Blank = 2
        Spancheck = 3

    WAVELENGTHS = (
        (450.0, "B"),
        (550.0, "G"),
        (700.0, "R"),
    )

    class _ZeroAverage:
        def __init__(self):
            self.sum: float = 0.0
            self.count: int = 0
            self.reset_counter: int = 0

        def __call__(self, add: float) -> None:
            if add is None or not isfinite(add):
                return
            self.sum += add
            self.count += 1
            self.reset_counter = 5

        def skip(self) -> None:
            if self.reset_counter == 0:
                return
            self.reset_counter -= 1
            if self.reset_counter > 0:
                return
            self.reset()

        def mean(self) -> float:
            if not self.count:
                return nan
            return self.sum / float(self.count)

        def reset(self) -> None:
            self.sum = 0.0
            self.count = 0
            self.reset_counter = 0

    class _Spancheck(SpancheckController):
        class Angle(SpancheckController.Angle):
            def __init__(self, spancheck: "Instrument._Spancheck"):
                super().__init__(spancheck)
                self.spancheck = spancheck
                self.measurement_counts = spancheck.MeasurementAverage()
                self.reference_counts = spancheck.MeasurementAverage()
                self.dark_counts = spancheck.MeasurementAverage()
                self.revolutions = spancheck.MeasurementAverage()

        class Result(SpancheckController.Result):
            class ForAngleWavelength(SpancheckController.Result.ForAngleWavelength):
                def output_data(self) -> typing.Dict[str, typing.Dict[str, typing.Any]]:
                    data: typing.Dict[str, typing.Dict[str, typing.Any]] = dict()
                    for i in range(len(self.result.angles)):
                        contents: typing.Dict[str, typing.Any] = dict()
                        data['total' if i == 0 else 'back'] = contents
                        for j in range(len(self.result.wavelengths)):
                            contents[Instrument.WAVELENGTHS[j][1]] = self.values[i][j]
                    return data

            class ForWavelength(SpancheckController.Result.ForWavelength):
                def output_data(self) -> typing.Dict[str, typing.Any]:
                    data: typing.Dict[str, typing.Any] = dict()
                    for i in range(len(self.result.wavelengths)):
                        data[Instrument.WAVELENGTHS[i][1]] = self.values[i]
                    return data

            def _calculate_counts(self, instrument: "Instrument", wavelength: float, raw: float, revs: float, width: float) -> float:
                K1 = instrument.active_parameters.K1(self.wavelength_index[wavelength])
                return instrument._calculate_counts(raw, revs, width, K1)
            
            def _calculate_sensitivity_factor(self, 
                                              air_rayleigh: float, air_measure: float, air_dark: float,
                                              gas_rayleigh: float, gas_measure: float, gas_dark: float) -> float:
                ratio = gas_rayleigh / air_rayleigh - 1.0
                if not isfinite(ratio) or abs(ratio) < 0.01:
                    return nan
                c_gas = self.to_stp(gas_measure - gas_dark, self.gas_density)
                c_air = self.to_stp(air_measure - air_dark, self.air_density)
                return (c_gas - c_air) / ratio

            def _calculate_K2(self, wavelength: float) -> float:
                air_measure = self.air_measurement_counts[0.0, wavelength]
                air_dark = self.air_dark_counts[0.0, wavelength]
                air_reference = self.air_reference_counts[0.0, wavelength]
                gas_measure = self.gas_measurement_counts[0.0, wavelength]
                gas_dark = self.gas_dark_counts[0.0, wavelength]
                gas_reference = self.gas_reference_counts[0.0, wavelength]

                div = (((gas_measure - gas_dark) / (gas_reference - gas_dark)) -
                       ((air_measure - air_dark) / (air_reference - air_dark)))
                if not isfinite(div) or div == 0.0:
                    return nan
                return self.K3_span[wavelength] / div

            def _calculate_K4(self, wavelength: float) -> float:
                k3_span = self.K3_span[wavelength]
                if not isfinite(k3_span) or k3_span == 0.0:
                    return nan

                k2 = self.K2[wavelength]
                air_back_measure = self.air_measurement_counts[90.0, wavelength]
                air_back_dark = self.air_dark_counts[90.0, wavelength]
                air_total_reference = self.air_reference_counts[0.0, wavelength]
                air_total_dark = self.air_dark_counts[0.0, wavelength]
                gas_back_measure = self.gas_measurement_counts[90.0, wavelength]
                gas_back_dark = self.gas_dark_counts[90.0, wavelength]
                gas_total_reference = self.gas_reference_counts[0.0, wavelength]
                gas_total_dark = self.gas_dark_counts[0.0, wavelength]

                return (k2 * (((gas_back_measure - gas_back_dark) /
                               (gas_total_reference - gas_total_dark)) -
                              ((air_back_measure - air_back_dark) /
                               (air_total_reference - air_total_dark)))) / k3_span

            def __init__(self, spancheck: "Instrument._Spancheck", air: "Instrument._Spancheck.Phase",
                         gas: "Instrument._Spancheck.Phase", gas_rayleigh_factor: float):
                super().__init__(spancheck, air, gas, gas_rayleigh_factor)

                self.air_revolutions = self.ForAngleWavelength(self, lambda angle, wavelength: (
                    float(self.air.wavelengths[wavelength].angles[angle].revolutions)
                ))
                self.air_measurement_counts = self.ForAngleWavelength(self, lambda angle, wavelength: self._calculate_counts(
                    spancheck.instrument, wavelength,
                    float(self.air.wavelengths[wavelength].angles[angle].measurement_counts),
                    self.air_revolutions[angle, wavelength], 140.0,
                ))
                self.air_reference_counts = self.ForAngleWavelength(self, lambda angle, wavelength: self._calculate_counts(
                    spancheck.instrument, wavelength,
                    float(self.air.wavelengths[wavelength].angles[angle].reference_counts),
                    self.air_revolutions[angle, wavelength], 40.0,
                ))
                self.air_dark_counts = self.ForAngleWavelength(self, lambda angle, wavelength: self._calculate_counts(
                    spancheck.instrument, wavelength,
                    float(self.air.wavelengths[wavelength].angles[angle].dark_counts),
                    self.air_revolutions[angle, wavelength], 60.0,
                ))

                self.gas_revolutions = self.ForAngleWavelength(self, lambda angle, wavelength: (
                    float(self.gas.wavelengths[wavelength].angles[angle].revolutions)
                ))
                self.gas_measurement_counts = self.ForAngleWavelength(self, lambda angle, wavelength: self._calculate_counts(
                    spancheck.instrument, wavelength,
                    float(self.gas.wavelengths[wavelength].angles[angle].measurement_counts),
                    self.gas_revolutions[angle, wavelength], 140.0,
                ))
                self.gas_reference_counts = self.ForAngleWavelength(self, lambda angle, wavelength: self._calculate_counts(
                    spancheck.instrument, wavelength,
                    float(self.gas.wavelengths[wavelength].angles[angle].reference_counts),
                    self.gas_revolutions[angle, wavelength], 40.0,
                ))
                self.gas_dark_counts = self.ForAngleWavelength(self, lambda angle, wavelength: self._calculate_counts(
                    spancheck.instrument, wavelength,
                    float(self.gas.wavelengths[wavelength].angles[angle].dark_counts),
                    self.gas_revolutions[angle, wavelength], 60.0,
                ))

                self.sensitivity_factor = self.ForAngleWavelength(self, lambda angle, wavelength: self._calculate_sensitivity_factor(
                    self.air_rayleigh_scattering[angle, wavelength],
                    self.air_measurement_counts[angle, wavelength],
                    self.air_dark_counts[angle, wavelength],
                    self.gas_rayleigh_scattering[angle, wavelength],
                    self.gas_measurement_counts[angle, wavelength],
                    self.gas_dark_counts[angle, wavelength],
                ))

                # m-1
                self.K3_span = self.ForWavelength(self, lambda wavelength: (
                    (self.to_ambient(self.gas_rayleigh_scattering[0.0, wavelength], self.gas_density) -
                     self.to_ambient(self.air_rayleigh_scattering[0.0, wavelength], self.air_density)) * 1E-6
                ))

                self.K2 = self.ForWavelength(self, self._calculate_K2)
                self.K4 = self.ForWavelength(self, self._calculate_K4)

                result_data = self.output_data()
                average_error = self.average_percent_error()
                result_data['percent_error']['average'] = average_error
                spancheck.instrument.data_spancheck_result(result_data, oneshot=True)
                spancheck.instrument.context.bus.log(
                    f"Spancheck completed with an average error of {average_error:.1f}%",
                    result_data)

                spancheck.instrument.data_PCTc([
                    self.percent_error[0.0, wavelength] for wavelength in self.wavelengths
                ])
                spancheck.instrument.data_PCTbc([
                    self.percent_error[90.0, wavelength] for wavelength in self.wavelengths
                ])

                spancheck.instrument.data_Cc([
                    self.sensitivity_factor[0.0, wavelength] for wavelength in self.wavelengths
                ])
                spancheck.instrument.data_Cbc([
                    self.sensitivity_factor[90.0, wavelength] for wavelength in self.wavelengths
                ])

            def output_data(self) -> typing.Dict[str, typing.Any]:
                data = super().output_data()
                data['sensitivity_factor'] = self.sensitivity_factor.output_data()
                data['calibration'] = {
                    'K2': self.K2.output_data(),
                    'K4': self.K2.output_data(),
                }
                data['counts'] = {
                    'air': {
                        'revolutions': self.air_revolutions.output_data(),
                        'measurement': self.air_measurement_counts.output_data(),
                        'reference': self.air_reference_counts.output_data(),
                        'dark': self.air_dark_counts.output_data(),
                    },
                    'gas': {
                        'revolutions': self.gas_revolutions.output_data(),
                        'measurement': self.gas_measurement_counts.output_data(),
                        'reference': self.gas_reference_counts.output_data(),
                        'dark': self.gas_dark_counts.output_data(),
                    },
                }
                return data

        def __init__(self, bus: BaseBusInterface, instrument: "Instrument"):
            super().__init__(bus)
            self.instrument = instrument
            self.valve_overriden: bool = False

        @property
        def wavelengths(self) -> typing.Iterable[float]:
            return [wl for wl, _ in self.instrument.WAVELENGTHS]

        @property
        def angles(self) -> typing.Iterable[float]:
            return [0.0, 90.0]

        async def set_filtered_air(self) -> None:
            await self.bus.set_bypass_held(False)
            if not self.instrument.writer:
                return
            async with self.instrument._pause_reports():
                if not self.valve_overriden:
                    await self.instrument._ok_command(b"VZ\r")
                    self.valve_overriden = True
                await self.instrument._ok_command(b"B255\r")
                if self.instrument._spancheck_valve:
                    await self.instrument._ok_command(b"SX0\r")
                    await self.instrument._ok_command(b"SX0\r")

        async def set_span_gas(self) -> None:
            await self.bus.set_bypass_held(True)
            if not self.instrument.writer:
                return
            async with self.instrument._pause_reports():
                await self.instrument._ok_command(b"B0\r")
                if self.instrument._spancheck_valve:
                    await self.instrument._ok_command(b"SX5000\r")
                    await self.instrument._ok_command(b"SX5000\r")

        def _restore_B(self) -> None:
            if not self.instrument.writer:
                return
            if self.instrument._apply_parameters:
                if not self.instrument._apply_parameters.B:
                    self.instrument._apply_parameters.B = self.instrument.active_parameters.B
            else:
                self._apply_parameters = Parameters(
                    B=self.instrument.active_parameters.B,
                )

        def abort_desynchronized(self) -> bool:
            if not super().abort_desynchronized():
                return False
            self._restore_B()
            self.instrument.notify_spancheck(False)
            return True

        async def initialize(self) -> None:
            self.instrument.notify_spancheck(True)

        async def abort(self) -> None:
            self._restore_B()
            self.instrument.notify_spancheck(False)
            await self.bus.set_bypass_held(False)
            if not self.instrument.writer:
                return
            async with self.instrument._pause_reports():
                if self.instrument._spancheck_valve:
                    await self.instrument._ok_command(b"SX0\r")
                    await self.instrument._ok_command(b"SX0\r")
                await self.instrument._ok_command(b"Z\r")
                # Don't need a valve command now, since the zero handles it
                self.valve_overriden = False

        async def complete(self) -> None:
            self._restore_B()
            self.instrument.notify_spancheck(False)
            await self.bus.set_bypass_held(False)
            if not self.instrument.writer:
                return
            async with self.instrument._pause_reports():
                await self.instrument._ok_command(b"Z\r")
                # Don't need a valve command now, since the zero handles it
                self.valve_overriden = False

    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self._report_interval: int = int(context.config.get('REPORT_INTERVAL', default=1))
        self._maximum_lamp_current: float = float(context.config.get('MAXIMUM_LAMP_CURRENT', default=7.0))
        self._spancheck_valve: bool = bool(context.config.get('SPANCHECK.VALVE', default=True))
        self._apply_parameters: typing.Optional[Parameters] = Parameters.default()
        self._apply_parameters.load(context.config.section('PARAMETERS'))

        self._zero_schedule: typing.Optional[Schedule] = None
        zeros = context.config.section_or_constant('ZERO')
        if not zeros and not isinstance(zeros, bool):
            self._zero_schedule = Schedule(LayeredConfiguration({
                'SCHEDULE': {'TIME': 'PT56M58S'}
            }))
        elif zeros:
            self._zero_schedule = Schedule(zeros)

        self._next_current_limit = time.time()

        self.active_parameters = Parameters()
        self.data_parameters = self.persistent("parameters", save_value=False)

        self.data_Tsample = self.input("Tsample")
        self.data_Usample = self.input("Usample")
        self.data_Psample = self.input("Psample")
        self.data_Tinlet = self.input("Tinlet")
        self.data_Uinlet = self.input("Uinlet")
        self.data_Vl = self.input("Vl")
        self.data_Al = self.input("Al")
        self.data_modetime = self.input("modetime")

        self.data_mode = self.persistent("modestring")
        self.data_sampling = self.persistent_enum("sampling", self.SamplingMode, send_to_bus=False)

        self.data_Tzero = self.persistent("Tzero")
        self.average_Tzero = self._ZeroAverage()
        self.data_Pzero = self.persistent("Pzero")
        self.average_Pzero = self._ZeroAverage()

        self.data_Bs_wavelength: typing.List[Input] = list()
        self.data_Bbs_wavelength: typing.List[Input] = list()
        self.data_Bsw_wavelength: typing.List[Persistent] = list()
        self.data_Bbsw_wavelength: typing.List[Persistent] = list()
        self.data_Bswd_wavelength: typing.List[Persistent] = list()
        self.data_Bbswd_wavelength: typing.List[Persistent] = list()
        self.data_Cs_wavelength: typing.List[Input] = list()
        self.data_Cbs_wavelength: typing.List[Input] = list()
        self.data_Cd_wavelength: typing.List[Input] = list()
        self.data_Cbd_wavelength: typing.List[Input] = list()
        self.data_Cf_wavelength: typing.List[Input] = list()
        for _, code in self.WAVELENGTHS:
            self.data_Bs_wavelength.append(self.input("Bs" + code))
            self.data_Bbs_wavelength.append(self.input("Bbs" + code))
            self.data_Bsw_wavelength.append(self.persistent("Bsw" + code))
            self.data_Bbsw_wavelength.append(self.persistent("Bbsw" + code))
            self.data_Bswd_wavelength.append(self.persistent("Bswd" + code))
            self.data_Bbswd_wavelength.append(self.persistent("Bbswd" + code))
            self.data_Cs_wavelength.append(self.input("Cs" + code))
            self.data_Cbs_wavelength.append(self.input("Cbs" + code))
            self.data_Cd_wavelength.append(self.input("Cd" + code))
            self.data_Cbd_wavelength.append(self.input("Cbd" + code))
            self.data_Cf_wavelength.append(self.input("Cf" + code))

        self.data_wavelength = self.persistent("wavelength", save_value=False, send_to_bus=False)
        self.data_wavelength([wl for wl, _ in self.WAVELENGTHS])
        self.data_Bs = self.input_array("Bs")  # Sent to the bus because it has the zero data removed
        self.data_Bbs = self.input_array("Bbs")
        self.data_Bsw = self.persistent("Bsw", send_to_bus=False)
        self.data_Bbsw = self.persistent("Bbsw", send_to_bus=False)
        self.data_Cs = self.input_array("Cs", send_to_bus=False)
        self.data_Cbs = self.input_array("Cbs", send_to_bus=False)
        self.data_Cd = self.input_array("Cd", send_to_bus=False)
        self.data_Cbd = self.input_array("Cbd", send_to_bus=False)
        self.data_Cf = self.input_array("Cf", send_to_bus=False)
        self.data_PCTc = self.persistent("PCTc", send_to_bus=False)
        self.data_PCTbc = self.persistent("PCTbc", send_to_bus=False)
        self.data_Cc = self.persistent("Cc", send_to_bus=False)
        self.data_Cbc = self.persistent("Cbc", send_to_bus=False)

        self.notify_backscatter_disabled = self.notification("backscatter_disabled")
        self.notify_blank = self.notification("blank")
        self.notify_zero = self.notification("zero")
        self.notify_spancheck = self.notification("spancheck")

        self.data_spancheck_result = self.persistent("spancheck_result")
        self._spancheck = self._Spancheck(self.context.bus, self)

        dimension_wavelength = self.dimension_wavelength(self.data_wavelength)
        self.bit_flags: typing.Dict[int, Instrument.Notification] = dict()

        self.report_D = self.report(
            self.variable_total_scattering(self.data_Bs, dimension_wavelength, code="Bs"),
            self.variable_back_scattering(self.data_Bbs, dimension_wavelength, code="Bbs"),

            auxiliary_variables=(
                [self.variable(s) for s in self.data_Bs_wavelength] +
                [self.variable(s) for s in self.data_Bbs_wavelength]
            )
        )
        self.report_Y = self.report(
            self.variable_air_pressure(self.data_Psample, "sample_pressure", code="P",
                                       attributes={'long_name': "measurement cell pressure"}),
            self.variable_air_temperature(self.data_Tsample, "sample_temperature", code="T",
                                          attributes={'long_name': "measurement cell temperature"}),
            self.variable_temperature(self.data_Tinlet, "inlet_temperature", code="Tu",
                                      attributes={'long_name': "inlet temperature"}),
            self.variable_air_rh(self.data_Usample, "sample_humidity", code="U",
                                 attributes={'long_name': "measurement cell relative humidity"}),
            self.variable_rh(self.data_Uinlet, "inlet_humidity", code="Uu", attributes={
                'long_name': "calculated inlet humidity",
                'coverage_content_type': 'referenceInformation',
            }),
            self.variable(self.data_Vl, "lamp_voltage", code="Vl", attributes={
                'long_name': "lamp supply voltage",
                'units': "V",
                'C_format': "%4.1f"
            }),
            self.variable(self.data_Al, "lamp_current", code="Al", attributes={
                'long_name': "lamp current",
                'units': "A",
                'C_format': "%4.1f"
            }),

            flags=[
                self.flag_bit(self.bit_flags, 0x0001, "lamp_power_error", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0002, "valve_fault"),
                self.flag_bit(self.bit_flags, 0x0004, "chopper_fault", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0008, "shutter_fault", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0010, "heater_unstable", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0020, "pressure_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0040, "sample_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0080, "inlet_temperature_out_of_range", is_warning=True),
                self.flag_bit(self.bit_flags, 0x0100, "rh_out_of_range"),
            ],
        )
        self.report_BGR = self.report(
            self.variable_array(self.data_Cs, dimension_wavelength, "scattering_counts", code="Cs", attributes={
                'long_name': "total scattering photon count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),
            self.variable_array(self.data_Cbs, dimension_wavelength, "backscattering_counts", code="Cbs", attributes={
                'long_name': "backwards hemispheric scattering photon count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),
            self.variable_array(self.data_Cf, dimension_wavelength, "reference_counts", code="Cf", attributes={
                'long_name': "reference shutter photon count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),
            self.variable_array(self.data_Cd, dimension_wavelength, "scattering_dark_counts", code="Cd", attributes={
                'long_name': "total scattering dark count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),
            self.variable_array(self.data_Cbd, dimension_wavelength, "backscattering_dark_counts", code="Cbd", attributes={
                'long_name': "backwards hemispheric scattering dark count rate",
                'units': "Hz",
                'C_format': "%7.0f"
            }),

            auxiliary_variables=(
                [self.variable(s) for s in self.data_Cs_wavelength] +
                [self.variable(s) for s in self.data_Cbs_wavelength] +
                [self.variable(s) for s in self.data_Cf_wavelength] +
                [self.variable(s) for s in self.data_Cd_wavelength] +
                [self.variable(s) for s in self.data_Cbd_wavelength]
            )
        )
        self.seen_BGR_reports: typing.Set[int] = set()
        self.report_mode = self.report(
            flags=[
                self.flag(self.notify_backscatter_disabled, 0x1000),
                self.flag(self.notify_blank, 0x4000),
                self.flag(self.notify_zero, 0x2000),
                self.flag(self.notify_spancheck),
            ],
            automatic=False,
        )

        self.instrument_state = self.change_event(
            self.state_string(self.data_mode, "mode", attributes={
                'long_name': "instrument mode string",
            }),
            self.state_enum(self.data_sampling, attributes={
                'long_name': "sampling mode",
            }),
        )

        self.zero_state = self.change_event(
            self.state_temperature(self.data_Tzero, "zero_temperature", code="Tw", attributes={
                'long_name': "measurement cell temperature during the zero"
            }),
            self.state_pressure(self.data_Pzero, "zero_pressure", code="Pw", attributes={
                'long_name': "measurement cell pressure during the zero"
            }),
            self.state_wall_total_scattering(self.data_Bsw, dimension_wavelength, code="Bsw"),
            self.state_wall_back_scattering(self.data_Bbsw, dimension_wavelength, code="Bbsw"),
            name="zero",
        )

        def at_stp(s):
            s.data.use_standard_pressure = True
            s.data.use_standard_temperature = True
            return s

        self.spancheck_state = self.change_event(
            at_stp(self.state_measurement_array(
                self.data_PCTc, dimension_wavelength, "scattering_percent_error", code="PCTc", attributes={
                    'long_name': "spancheck total light scattering percent error",
                    'units': "%",
                    'C_format': "%6.2f"
                })),
            at_stp(self.state_measurement_array(
                self.data_PCTbc, dimension_wavelength, "backscattering_percent_error", code="PCTbc", attributes={
                    'long_name': "spancheck backwards hemispheric light scattering percent error",
                    'units': "%",
                    'C_format': "%6.2f"
                })),
            at_stp(self.state_measurement_array(
                self.data_Cc, dimension_wavelength, "scattering_sensitivity_factor", code="Cc", attributes={
                    'long_name': "total photon count rate attributable to Rayleigh scattering by air at STP",
                    'units': "Hz",
                    'C_format': "%7.1f"
                })),
            at_stp(self.state_measurement_array(
                self.data_Cbc, dimension_wavelength, "backscattering_sensitivity_factor", code="Cbc", attributes={
                    'long_name': "backwards hemispheric photon count rate attributable to Rayleigh scattering by air at STP",
                    'units': "Hz",
                    'C_format': "%7.1f"
                })),
            name="spancheck",
        )
        self.spancheck_state.data_record.standard_temperature = 0.0
        self.spancheck_state.data_record.standard_pressure = ONE_ATM_IN_HPA

        self.active_parameters.record(self.context.data.constant_record("parameters"), dimension_wavelength)

        self._zero_request = False
        self.context.bus.connect_command('start_zero', self.start_zero)
        self.context.bus.connect_command('set_parameters', self.set_parameters)

    async def _read_parameter(self, command: bytes) -> bytes:
        self.writer.write(command + b"\r")
        await self.writer.drain()
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        if data == b"ERROR":
            raise CommunicationsError(f"error reading {command}")
        return data

    async def _write_parameter(self, command: bytes) -> None:
        self.writer.write(command + b"\r")
        await self.writer.drain()
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        if data == b"OK":
            return
        if data == b"ERROR" or data == b"FAULT":
            raise CommunicationsError(f"error setting parameter {command}: {data}")

    async def _set_pending_parameters(self) -> None:
        apply = self._apply_parameters
        self._apply_parameters = None
        try:
            await self.active_parameters.apply_changes(apply, self._read_parameter, self._write_parameter)
            apply = None
        finally:
            if apply is not None:
                if self._apply_parameters:
                    apply.overlay(self._apply_parameters)
                    self._apply_parameters = apply
                else:
                    self._apply_parameters = apply

    async def _ok_command(self, command: bytes) -> None:
        self.writer.write(command)
        await self.writer.drain()
        data: bytes = await wait_cancelable(self.read_line(), 2.0)
        if data != b"OK":
            raise CommunicationsError(f"invalid command response {data}")

    async def start_communications(self) -> None:
        self.seen_BGR_reports.clear()
        self._zero_request = False
        self._spancheck.abort_desynchronized()

        if self._zero_schedule:
            discard_zero = self._zero_schedule.current()
            discard_zero.activate()

        if self.writer:
            async def retry_ok_command(command: bytes, retries: int = 5) -> None:
                self.writer.write(command)
                await self.writer.drain()
                for i in range(retries):
                    try:
                        data: bytes = await wait_cancelable(self.read_line(), 2.0)
                    except asyncio.TimeoutError:
                        self.writer.write(command)
                        await self.writer.drain()
                        continue
                    data = data.strip()
                    if data == b"OK":
                        return
                    elif data == b"ERROR":
                        await asyncio.sleep(0.5)
                        self.writer.write(command)
                        await self.writer.drain()
                raise CommunicationsError("command retries exhausted")

            # Stop any unpolled
            self.writer.write(b"\r\r\r\r\r\r\r\r\r\r\rUE\r")
            await self.writer.drain()
            await self.drain_reader(self._report_interval + 2.0)
            try:
                await retry_ok_command(b"UE\r")
            except CommunicationsError:
                self.writer.write(b"PU\r")
                await self.drain_reader(5.0)
                await retry_ok_command(b"UE\r")

            # Set comma delimiter
            await self._ok_command(b"SB0,0\r")
            if self._spancheck_valve:
                # Set spancheck valve
                await self._ok_command(b"SX0\r")
            # Change the zero valve back if needed
            if self._spancheck.valve_overriden:
                await self._ok_command(b"VN\r")
                self._spancheck.valve_overriden = False

            self.writer.write(b"RV\r")
            await self.writer.drain()
            data: bytes = await wait_cancelable(self.read_line(), 2.0)
            self.set_firmware_version(data)

            # Enable record types
            await self._ok_command(b"UT1\r")
            await self._ok_command(b"UD1\r")
            await self._ok_command(b"UY1\r")
            await self._ok_command(b"UP3\r")
            await self._ok_command(b"UZ1\r")

            ts = time.gmtime()
            await self._ok_command(f"STT,{ts.tm_year},{ts.tm_mon},{ts.tm_mday},"
                             f"{ts.tm_hour},{ts.tm_min},{ts.tm_sec}\r".encode('ascii'))

            await self.active_parameters.read(self._read_parameter)
            if self._apply_parameters:
                await self._set_pending_parameters()
            if self.active_parameters.SL:
                self.set_instrument_info('calibration', self.active_parameters.SL)
            self.data_parameters(self.active_parameters.persistent(), oneshot=True)

            async def process_initial_record(command: bytes, code: bytes,
                                             processor: typing.Callable[[ typing.List[bytes]], None]):
                self.writer.write(command)
                await self.writer.drain()
                data: bytes = await wait_cancelable(self.read_line(), 2.0)
                fields = data.split(b',')
                if len(fields) < 2 or fields[0].strip() != code:
                    raise CommunicationsError(f"invalid read for {command} with response {data}")
                del fields[0]
                processor(fields)

            await process_initial_record(b"RY\r", b"Y", self._process_Y)
            await process_initial_record(b"RD\r", b"D", self._process_D)
            if self.data_Bsw.value is None:
                _LOGGER.debug("No persistent zero, reading instrument saved values")
                await process_initial_record(b"RZ\r", b"Z", self._process_Z)

            self.writer.write(b"UB\r")
            await self.writer.drain()
            await self.drain_reader(0.5)

        # Flush the first record
        await self.drain_reader(0.5)
        await wait_cancelable(self.read_line(), self._report_interval * 3 + 5)

        # Process a valid record
        await self.communicate()

    def _emit_BGR(self) -> None:
        self.data_Cs([float(v) for v in self.data_Cs_wavelength])
        self.data_Cbs([float(v) for v in self.data_Cbs_wavelength])
        self.data_Cd([float(v) for v in self.data_Cd_wavelength])
        self.data_Cbd([float(v) for v in self.data_Cbd_wavelength])
        self.data_Cf([float(v) for v in self.data_Cf_wavelength])
        self.report_BGR()

    def _advance_BGR(self, wavelength: typing.Optional[int] = None) -> None:
        if wavelength is None or wavelength in self.seen_BGR_reports:
            if self.seen_BGR_reports:
                self._emit_BGR()
            self.seen_BGR_reports.clear()
        if wavelength is not None:
            self.seen_BGR_reports.add(wavelength)
            if len(self.seen_BGR_reports) == 3:
                self.seen_BGR_reports.clear()
                self._emit_BGR()

    def _process_T(self, fields: typing.List[bytes]) -> None:
        try:
            (
                year, month, day,
                hour, minute, second
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid fields in T record {fields}")

        try:
            year = int(year)
            if year < 1900 or year > 2999:
                raise CommunicationsError(f"invalid year {year}")
            month = int(month)
            day = int(day)
            hour = int(hour)
            minute = int(minute)
            second = int(second)
            datetime.datetime(year, month, day, hour, minute, second, tzinfo=datetime.timezone.utc)
        except ValueError as e:
            raise CommunicationsError from e

        self._advance_BGR()

    @property
    def _scatterings_valid(self) -> bool:
        if self.data_sampling.value != self.SamplingMode.Normal:
            return False
        if bool(self.notify_blank):
            return False
        if bool(self.notify_zero):
            return False
        return True

    def _process_D(self, fields: typing.List[bytes]) -> None:
        try:
            (
                mode, remaining_time,
                *channels
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid fields in D record {fields}")
        if len(channels) != 6:
            raise CommunicationsError(f"invalid fields in D record {fields}")
        if len(mode) != 4:
            raise CommunicationsError(f"invalid sampling mode in {fields}")

        self.data_mode(mode.decode('ascii'))
        self.data_modetime(parse_number(remaining_time))

        sampling_mode = self.SamplingMode.Normal
        mode_string = mode[0:1]
        if mode_string == b'N':
            self.notify_blank(False)
            self.notify_zero(False)
        elif mode_string == b'B':
            self.notify_blank(True)
            self.notify_zero(False)
            sampling_mode = self.SamplingMode.Blank
        elif mode_string == b'Z':
            self.notify_blank(False)
            self.notify_zero(True)
            sampling_mode = self.SamplingMode.Zero
        if self._spancheck.is_running:
            sampling_mode = self.SamplingMode.Spancheck
            self.notify_spancheck(True)
        else:
            self.notify_spancheck(False)
        self.data_sampling(sampling_mode)

        mode_string = mode[1:2]
        if mode_string == b'B':
            self.notify_backscatter_disabled(False)
        elif mode_string == b'T':
            self.notify_backscatter_disabled(True)

        for i in range(3):
            self.data_Bs_wavelength[i](parse_number(channels[i]) * 1E6)
            if not bool(self.notify_backscatter_disabled):
                self.data_Bbs_wavelength[i](parse_number(channels[i+3]) * 1E6)
            else:
                self.data_Bbs_wavelength[i](nan)

        if self._scatterings_valid:
            self.data_Bs([float(c) for c in self.data_Bs_wavelength])
            self.data_Bbs([float(c) for c in self.data_Bbs_wavelength])
        else:
            self.data_Bs([nan for _ in self.data_Bs_wavelength])
            self.data_Bbs([nan for _ in self.data_Bbs_wavelength])

        spancheck_phase = self._spancheck.active_phase
        if spancheck_phase:
            for i in range(len(self.WAVELENGTHS)):
                wl = spancheck_phase[self.WAVELENGTHS[i][0]]
                wl[0.0].scattering(float(self.data_Bs_wavelength[i]))
                wl[90.0].scattering(float(self.data_Bbs_wavelength[i]))

        self._advance_BGR()
        self.report_D()
        self.report_mode()

    def _process_Y(self, fields: typing.List[bytes]) -> None:
        try:
            (
                _,  # Green reference
                Psample, Tsample, Tinlet, Usample, Vl, Al,
                _,  # BNC mV
                flags
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid fields in Y record {fields}")

        self.data_Psample(parse_number(Psample))

        Vl = parse_number(Vl)
        Al = parse_number(Al)

        if Al > self._maximum_lamp_current and self._next_current_limit < time.time():
            measured_power = Vl * Al
            setpoint_power = self.active_parameters.SP

            reduced_power = measured_power
            if setpoint_power and reduced_power > setpoint_power:
                reduced_power = float(setpoint_power)
            reduced_power = int(floor(reduced_power * 0.9))
            if reduced_power < 0 or reduced_power >= 150:
                reduced_power = 0

            if not self._apply_parameters:
                self._apply_parameters = Parameters()
            self._apply_parameters.SP = reduced_power

            self.context.bus.log(
                f"Lamp current too high.  Reducing power to {reduced_power} watts.  Replace lamp immediately.", {
                    "measured_power": measured_power,
                    "setpoint_power": setpoint_power,
                    "reduced_power": reduced_power,
                    "current": Vl,
                    "voltage": Al,
                }, type=BaseBusInterface.LogType.ERROR)
            _LOGGER.warning(f"Lamp current limiting applied, reducing power to {reduced_power} watts")

            self._next_current_limit = time.time() + 60.0

        self.data_Vl(Vl)
        self.data_Al(Al)

        Tsample = parse_number(Tsample)
        if Tsample > 150.0:
            Tsample = temperature_k_to_c(Tsample)
        Tsample = self.data_Tsample(Tsample)

        Tinlet = parse_number(Tinlet)
        if Tinlet > 150.0:
            Tinlet = temperature_k_to_c(Tinlet)
        Tinlet = self.data_Tinlet(Tinlet)

        Usample = self.data_Usample(parse_number(Usample))

        self.data_Uinlet(extrapolate_rh(Tsample, Usample, Tinlet))

        parse_flags_bits(flags, self.bit_flags)

        if bool(self.notify_zero):
            self.average_Tzero(Tsample)
            self.average_Pzero(float(self.data_Psample))
        elif not bool(self.notify_blank):
            self.average_Tzero.skip()
            self.average_Pzero.skip()

        spancheck_phase = self._spancheck.active_phase
        if spancheck_phase:
            spancheck_phase.temperature(Tsample)
            spancheck_phase.pressure(float(self.data_Psample))

        self._advance_BGR()
        self.report_Y()

    @staticmethod
    def _calculate_counts(raw: float, revs: float, width: float, K1: float) -> float:
        if raw is None or revs is None or K1 is None:
            return nan
        if not isfinite(raw) or not isfinite(revs) or not isfinite(K1):
            return nan
        if revs <= 0.0:
            return nan
        Cs = (360.0 * raw * 22.994) / (width * revs)
        Cs *= (Cs * K1 * 1E-12 + 1.0)
        return Cs

    def _process_BGR(self, fields: typing.List[bytes], wavelength: int) -> None:
        try:
            (
                Cf, Cs, Cd, total_revs,
                _,  # Backscatter reference
                Cbs, Cbd, back_revs,
                _,  # Sample pressure
                _   # Sample temperature
            ) = fields
        except ValueError:
            raise CommunicationsError(f"invalid fields in photon count {wavelength} record {fields}")

        K1 = self.active_parameters.K1(wavelength)
        if K1 is None or not isfinite(K1):
            K1 = 20000

        total_revs = parse_number(total_revs)
        Cf = parse_number(Cf)
        Cs = parse_number(Cs)
        Cd = parse_number(Cd)
        self.data_Cf_wavelength[wavelength](self._calculate_counts(Cf, total_revs, 40.0, K1))
        self.data_Cs_wavelength[wavelength](self._calculate_counts(Cs, total_revs, 140.0, K1))
        self.data_Cd_wavelength[wavelength](self._calculate_counts(Cd, total_revs, 60.0, K1))

        back_revs = parse_number(back_revs)
        Cbs = parse_number(Cbs)
        Cbd = parse_number(Cbd)
        self.data_Cbs_wavelength[wavelength](self._calculate_counts(Cbs, back_revs, 140.0, K1))
        self.data_Cbd_wavelength[wavelength](self._calculate_counts(Cbd, back_revs, 60.0, K1))

        spancheck_phase = self._spancheck.active_phase
        if spancheck_phase:
            wl: "Instrument._Spancheck.Wavelength" = spancheck_phase[self.WAVELENGTHS[wavelength][0]]
            wl[0.0].measurement_counts(Cs)
            wl[0.0].reference_counts(Cf)
            wl[0.0].dark_counts(Cd)
            wl[0.0].dark_counts(total_revs)
            wl[90.0].measurement_counts(Cbs)
            wl[90.0].reference_counts(Cf)
            wl[90.0].dark_counts(Cbd)
            wl[90.0].dark_counts(back_revs)

        self._advance_BGR(wavelength)

    @staticmethod
    def _calculate_wall_scattering(Bswr: float, Bsr: float, Bsr_fraction: float = 1.0) -> float:
        if Bswr is None or Bsr is None:
            return nan
        if not isfinite(Bswr) or not isfinite(Bsr):
            return nan
        return Bswr - Bsr * Bsr_fraction

    @staticmethod
    def _calculate_zero_change(prior: float, current: float) -> float:
        if prior is None or current is None:
            return nan
        if not isfinite(prior) or not isfinite(current):
            return nan
        return current - prior

    def _process_Z(self, fields: typing.List[bytes]) -> None:
        if len(fields) != 9:
            raise CommunicationsError(f"invalid fields in Z record {fields}")

        self.data_Tzero(self.average_Tzero.mean())
        self.data_Pzero(self.average_Pzero.mean())
        self.average_Tzero.reset()
        self.average_Pzero.reset()

        for i in range(3):
            Bswr = parse_number(fields[i]) * 1E6
            Bbswr = parse_number(fields[i+3]) * 1E6
            Bsr = parse_number(fields[i+6]) * 1E6

            prior_Bsw = self.data_Bsw_wavelength[i].value
            current_Bsw = self.data_Bsw_wavelength[i](
                self._calculate_wall_scattering(Bswr, Bsr),
                oneshot=True,
            )
            self.data_Bswd_wavelength[i](self._calculate_zero_change(prior_Bsw, current_Bsw))

            prior_Bbsw = self.data_Bbsw_wavelength[i].value
            current_Bbsw = self.data_Bbsw_wavelength[i](
                self._calculate_wall_scattering(Bbswr, Bsr, 0.5),
                oneshot=True,
            )
            self.data_Bbswd_wavelength[i](self._calculate_zero_change(prior_Bbsw, current_Bbsw))

        self.data_Bsw([c.value for c in self.data_Bsw_wavelength], oneshot=True)
        self.data_Bbsw([c.value for c in self.data_Bbsw_wavelength], oneshot=True)
        _LOGGER.debug("Zero processed")

    @asynccontextmanager
    async def _pause_reports(self):
        if not self.writer:
            return

        self.writer.write(b"UE\r")
        await self.writer.drain()
        await self.drain_reader(self._report_interval + 0.5)

        for i in range(5):
            self.writer.write(b"UE\r")
            await self.writer.drain()
            try:
                data: bytes = await wait_cancelable(self.read_line(), 2.0)
            except asyncio.TimeoutError:
                continue
            data = data.strip()
            if data == b"OK":
                break

            await self.drain_reader(0.25)
        else:
            raise CommunicationsError("error stopping unpolled reports")

        yield

        self.writer.write(b"UB\r")
        await self.drain_reader(self._report_interval + 0.5)
        await wait_cancelable(self.read_line(), self._report_interval + 5)

    async def _update_state(self) -> None:
        if self._zero_schedule:
            now = time.time()
            zero = self._zero_schedule.current(now)
            if zero.activate(now):
                if self.data_sampling.value != self.SamplingMode.Normal:
                    _LOGGER.debug("Ignoring scheduled zero while not in normal operating mode")
                else:
                    _LOGGER.debug("Automatic zero scheduled")
                    self._zero_request = False

        if self._spancheck.is_running:
            self._zero_request = False
            return

        do_update = False
        do_update = do_update or self._zero_request
        do_update = do_update or (self._apply_parameters is not None)

        if not do_update:
            return    

        async with self._pause_reports():
            if self._apply_parameters:
                if self.writer:
                    await self._set_pending_parameters()
                    self.data_parameters(self.active_parameters.persistent(), oneshot=True)
                else:
                    _LOGGER.warning("Unable to change parameters")
            if self._zero_request:
                self._zero_request = False
                if self.writer:
                    _LOGGER.debug("Sending zero start command")
                    await self._ok_command(b"Z\r")
                else:
                    _LOGGER.warning("Unable to start zero")

    def start_zero(self, _) -> None:
        if self.data_sampling.value != self.SamplingMode.Normal:
            _LOGGER.debug("Discarding zero request while not in normal operation mode")
            return
        self._zero_request = True

    def set_parameters(self, parameters: typing.Dict[str, typing.Any]) -> None:
        if not isinstance(parameters, dict):
            return
        to_set = Parameters()
        to_set.load(parameters)
        if self._apply_parameters:
            self._apply_parameters.overlay(to_set)
        else:
            self._apply_parameters = to_set

    async def communicate(self) -> None:
        await self._spancheck()
        await self._update_state()

        line: bytes = await wait_cancelable(self.read_line(), self._report_interval + 5)
        if len(line) < 3:
            raise CommunicationsError

        fields = _FIELD_SPLIT.split(line.strip())
        if len(fields) < 2:
            raise CommunicationsError(f"no record type in {line}")

        record_type = fields[0].strip()
        del fields[0]

        if record_type == b'T':
            self._process_T(fields)
        elif record_type == b'D':
            self._process_D(fields)
        elif record_type == b'Y':
            self._process_Y(fields)
        elif record_type == b'B':
            self._process_BGR(fields, 0)
        elif record_type == b'G':
            self._process_BGR(fields, 1)
        elif record_type == b'R':
            self._process_BGR(fields, 2)
        elif record_type == b'Z':
            self._process_Z(fields)
        else:
            raise CommunicationsError(f"invalid record type in {line}")
