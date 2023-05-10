import typing
import logging
from collections import OrderedDict
from enum import Enum
from math import isfinite, nan
from forge.rayleigh import rayleigh_scattering, CO2
from forge.units import ZERO_C_IN_K, ONE_ATM_IN_HPA
from .base import BaseBusInterface

_LOGGER = logging.getLogger(__name__)


class Spancheck:
    class MeasurementAverage:
        def __init__(self):
            self.sum: float = 0
            self.count: int = 0

        def __call__(self, add: float) -> None:
            if add is None or not isfinite(add):
                return
            self.sum += add
            self.count += 1

        def __float__(self) -> float:
            if not self.count:
                return nan
            return self.sum / float(self.count)

    class Angle:
        def __init__(self, spancheck: "Spancheck"):
            self.spancheck = spancheck
            self.scattering = spancheck.MeasurementAverage()

    class Wavelength:
        def __init__(self, spancheck: "Spancheck", angles: typing.Dict[float, "Spancheck.Angle"]):
            self.spancheck = spancheck
            self.angles = angles

        def __getitem__(self, item: float):
            return self.angles[item]

    class Phase:
        def __init__(self, spancheck: "Spancheck", wavelengths: typing.Dict[float, "Spancheck.Wavelength"]):
            self.spancheck = spancheck
            self.wavelengths = wavelengths
            self.temperature = spancheck.MeasurementAverage()
            self.pressure = spancheck.MeasurementAverage()

        def __getitem__(self, item: float):
            return self.wavelengths[item]

    class Result:
        class ForAngleWavelength:
            def __init__(self, result: "Spancheck.Result",
                         calculate: typing.Callable[[float, float], float] = None):
                self.result = result
                self.values: typing.List[typing.List[float]] = list()
                for angle in self.result.angles:
                    data: typing.List[float] = list()
                    self.values.append(data)
                    for wavelength in self.result.wavelengths:
                        if calculate:
                            data.append(calculate(angle, wavelength))
                        else:
                            data.append(nan)

            def __getitem__(self, item: typing.Tuple[float, float]):
                idx_angle = self.result.angle_index[item[0]]
                idx_wavelength = self.result.wavelength_index[item[1]]
                return self.values[idx_angle][idx_wavelength]

            def output_data(self) -> typing.Dict[str, typing.Dict[str, typing.Any]]:
                data: typing.Dict[str, typing.Dict[str, typing.Any]] = dict()
                for i in range(len(self.result.angles)):
                    contents: typing.Dict[str, typing.Any] = dict()
                    data[str(int(self.result.angles[i]))] = contents
                    for j in range(len(self.result.wavelengths)):
                        contents[str(int(self.result.wavelengths[j]))] = self.values[i][j]
                return data

        class ForWavelength:
            def __init__(self, result: "Spancheck.Result",
                         calculate: typing.Callable[[float], float] = None):
                self.result = result
                self.values: typing.List[float] = list()
                for wavelength in self.result.wavelengths:
                    if calculate:
                        self.values.append(calculate(wavelength))
                    else:
                        self.values.append(nan)

            def __getitem__(self, item: float):
                idx_wavelength = self.result.wavelength_index[item]
                return self.values[idx_wavelength]

            def output_data(self) -> typing.Dict[str, typing.Any]:
                data: typing.Dict[str, typing.Any] = dict()
                for i in range(len(self.result.wavelengths)):
                    data[str(int(self.result.wavelengths[i]))] = self.values[i]
                return data

        @staticmethod
        def _density(t: float, p: float) -> float:
            if not isfinite(t) or not isfinite(p):
                return nan
            if t < 150.0:
                t += ZERO_C_IN_K
            if t < 150.0 or t > 350.0 or p < 10.0 or p > 2000.0:
                return nan
            return (p / ONE_ATM_IN_HPA) * (ZERO_C_IN_K / t)

        @staticmethod
        def to_stp(value: float, density: float) -> float:
            if not isfinite(value) or not isfinite(density):
                return nan
            if density < 0.01:
                return nan
            return value / density

        @staticmethod
        def to_ambient(value: float, density: float) -> float:
            if not isfinite(value) or not isfinite(density):
                return nan
            if density < 0.01:
                return nan
            return value * density

        def __init__(self, spancheck: "Spancheck", air: "Spancheck.Phase",
                     gas: "Spancheck.Phase", gas_rayleigh_factor: float,
                     measurement_stp_t: typing.Optional[float] = None,
                     measurement_stp_p: typing.Optional[float] = None):
            self.air = air
            self.gas = gas
            self.gas_rayleigh_factor = gas_rayleigh_factor

            self.wavelengths = list(spancheck.wavelengths)
            self.angles = list(spancheck.angles)

            self.wavelength_index: typing.Dict[float, int] = OrderedDict()
            for i in range(len(self.wavelengths)):
                self.wavelength_index[self.wavelengths[i]] = i
            self.angle_index: typing.Dict[float, int] = OrderedDict()
            for i in range(len(self.angles)):
                self.angle_index[self.angles[i]] = i

            self.air_temperature = float(self.air.temperature)
            self.air_pressure = float(self.air.pressure)
            self.air_density = self._density(self.air_temperature, self.air_pressure)

            if measurement_stp_t is None and measurement_stp_p is None:
                air_normalization_density = self.air_density
            else:
                air_normalization_t = measurement_stp_t
                if air_normalization_t is None:
                    air_normalization_t = self.air_temperature
                air_normalization_p = measurement_stp_p
                if air_normalization_p is None:
                    air_normalization_p = self.air_temperature
                air_normalization_density = self._density(air_normalization_t, air_normalization_p)
            self.air_scattering = self.ForAngleWavelength(self, lambda angle, wavelength: (
                self.to_stp(
                    float(self.air.wavelengths[wavelength].angles[angle].scattering),
                    air_normalization_density
                )
            ))
            self.air_rayleigh_scattering = self.ForAngleWavelength(self, lambda angle, wavelength: (
                rayleigh_scattering(wavelength, angle)
            ))

            self.gas_temperature = float(self.gas.temperature)
            self.gas_pressure = float(self.gas.pressure)
            self.gas_density = self._density(self.gas_temperature, self.gas_pressure)
            
            if measurement_stp_t is None and measurement_stp_p is None:
                gas_normalization_density = self.gas_density
            else:
                gas_normalization_t = measurement_stp_t
                if gas_normalization_t is None:
                    gas_normalization_t = self.gas_temperature
                gas_normalization_p = measurement_stp_p
                if gas_normalization_p is None:
                    gas_normalization_p = self.gas_temperature
                gas_normalization_density = self._density(gas_normalization_t, gas_normalization_p)
            self.gas_scattering = self.ForAngleWavelength(self, lambda angle, wavelength: (
                self.to_stp(
                    float(self.gas.wavelengths[wavelength].angles[angle].scattering),
                    gas_normalization_density
                )
            ))
            self.gas_rayleigh_scattering = self.ForAngleWavelength(self, lambda angle, wavelength: (
                    rayleigh_scattering(wavelength, angle) * self.gas_rayleigh_factor
            ))

            self.corrected_scattering = self.ForAngleWavelength(self, lambda angle, wavelength: (
                    self.gas_scattering[angle, wavelength] -
                    self.air_scattering[angle, wavelength] +
                    self.air_rayleigh_scattering[angle, wavelength]
            ))
            self.percent_error = self.ForAngleWavelength(self, lambda angle, wavelength: (
                    (self.corrected_scattering[angle, wavelength] /
                     self.gas_rayleigh_scattering[angle, wavelength] - 1.0) * 100.0
            ))

        def output_data(self) -> typing.Dict[str, typing.Any]:
            data: typing.Dict[str, typing.Any] = {
                'temperature': {
                    'air': self.air_temperature,
                    'gas': self.gas_temperature,
                },
                'pressure': {
                    'air': self.air_pressure,
                    'gas': self.gas_pressure,
                },
                'gas_rayleigh_factor': self.gas_rayleigh_factor,
                'scattering': {
                    'air': self.air_scattering.output_data(),
                    'gas': self.gas_scattering.output_data(),
                },
                'corrected_scattering': self.corrected_scattering.output_data(),
                'percent_error': self.percent_error.output_data(),
            }
            return data

        def average_percent_error(self) -> float:
            s = 0.0
            c = 0
            for angle in self.percent_error.values:
                for wl in angle:
                    if wl is None or not isfinite(wl):
                        continue
                    s += abs(wl)
                    c += 1
            if not c:
                return nan
            return s / c

    class _MeasurementPhase(Enum):
        Inactive = 0
        Active = 1
        Gas = 2
        Air = 3

    def __init__(self, bus: BaseBusInterface):
        self.bus = bus

        self._measurement: Spancheck._MeasurementPhase = Spancheck._MeasurementPhase.Inactive
        self._gas_measurement: typing.Optional["Spancheck.Phase"] = None
        self._air_measurement: typing.Optional["Spancheck.Phase"] = None

        self._pending_operations: typing.List[typing.Callable[[], typing.Awaitable]] = list()

        self.bus.connect_command('_spancheck_control', self._control)
        self.bus.connect_command('_spancheck_calculate', self._calculate)

        self.last_result: typing.Optional["Spancheck.Result"] = None

    def create_phase(self, is_air: bool) -> "Spancheck.Phase":
        wavelengths: typing.Dict[float, "Spancheck.Wavelength"] = dict()
        for wl in self.wavelengths:
            angles: typing.Dict[float, "Spancheck.Angle"] = dict()
            for a in self.angles:
                angles[a] = self.Angle(self)
            wavelengths[wl] = self.Wavelength(self, angles)
        return self.Phase(self, wavelengths)

    def _control(self, action: str) -> None:
        if not isinstance(action, str):
            return
        if action == 'initialize':
            self.last_result = None
            self._measurement = Spancheck._MeasurementPhase.Active
            self._gas_measurement = self.create_phase(False)
            self._air_measurement = self.create_phase(True)
            self._pending_operations.append(self.initialize)
            _LOGGER.debug("Spancheck initializing")
            return

        if self._measurement == Spancheck._MeasurementPhase.Inactive:
            _LOGGER.debug(f"Discarding spancheck action {action} outside of active spancheck")
            return

        if action == 'abort':
            self._gas_measurement = None
            self._air_measurement = None
            self._pending_operations.append(self.abort)
            self._pending_operations.append(self._to_inactive)
            _LOGGER.debug("Spancheck aborting")
        elif action == 'complete':
            self._pending_operations.append(self.complete)
            self._pending_operations.append(self._to_inactive)
            _LOGGER.debug("Spancheck completed normally")
        elif action == 'air_flush':
            self._measurement = Spancheck._MeasurementPhase.Active
            self._pending_operations.append(self.set_filtered_air)
            _LOGGER.debug("Spancheck starting air flush")
        elif action == 'gas_flush':
            self._measurement = Spancheck._MeasurementPhase.Active
            self._pending_operations.append(self.set_span_gas)
            _LOGGER.debug("Spancheck starting gas flush")
        elif action == 'gas_sample':
            self._measurement = Spancheck._MeasurementPhase.Gas
            _LOGGER.debug("Spancheck starting gas measurement")
        elif action == 'air_sample':
            self._measurement = Spancheck._MeasurementPhase.Air
            _LOGGER.debug("Spancheck starting air measurement")

    def _calculate(self, parameters: typing.Dict[str, typing.Any]) -> None:
        if not isinstance(parameters, dict):
            return
        if self._gas_measurement is None or self._air_measurement is None:
            return

        try:
            gas_factor = float(parameters.get('gas_factor', CO2))
            if not isfinite(gas_factor) or gas_factor <= 0.0:
                raise ValueError
        except (TypeError, ValueError):
            return

        self.last_result = self.Result(self, self._air_measurement, self._gas_measurement, gas_factor)

    @property
    def wavelengths(self) -> typing.Iterable[float]:
        raise NotImplementedError

    @property
    def angles(self) -> typing.Iterable[float]:
        raise NotImplementedError

    @property
    def active_phase(self) -> typing.Optional["Spancheck.Phase"]:
        if self._measurement == Spancheck._MeasurementPhase.Air:
            return self._air_measurement
        elif self._measurement == Spancheck._MeasurementPhase.Gas:
            return self._gas_measurement
        return None

    @property
    def is_running(self) -> bool:
        return self._measurement != Spancheck._MeasurementPhase.Inactive

    async def _to_inactive(self) -> None:
        # Make sure we do this after complete, so we don't have a "gap" of valid between complete into blanking
        self._measurement = Spancheck._MeasurementPhase.Inactive

    def abort_desynchronized(self) -> bool:
        if self._measurement == Spancheck._MeasurementPhase.Inactive:
            return False

        _LOGGER.warning("Spancheck aborted due to de-synchronization")
        self.bus.log("Spancheck aborted due to de-synchronization",
                     type=BaseBusInterface.LogType.ERROR)

        self._measurement = Spancheck._MeasurementPhase.Inactive
        self._pending_operations.clear()
        self._gas_measurement = None
        self._air_measurement = None
        return True

    async def set_filtered_air(self) -> None:
        raise NotImplementedError

    async def set_span_gas(self) -> None:
        raise NotImplementedError

    async def initialize(self) -> None:
        pass

    async def abort(self) -> None:
        await self.bus.set_bypass_held(False)

    async def complete(self) -> None:
        await self.bus.set_bypass_held(False)

    async def __call__(self) -> None:
        ops = list(self._pending_operations)
        self._pending_operations.clear()
        for op in ops:
            await op()
