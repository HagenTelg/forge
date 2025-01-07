import typing
import logging
import numpy as np
from abc import ABC, abstractmethod
from json import loads as from_json
from math import nan, isfinite
from netCDF4 import Dataset
from forge.data.flags import declare_flag
from forge.data.structure.variable import variable_flags
from forge.data.structure.timeseries import variable_coordinates
from forge.solver import polynomial as solve_polynomial
from .selection import Selection

_LOGGER = logging.getLogger(__name__)


def _conditional_index(var, time_selection: typing.Union[slice, np.ndarray], apply_selection: typing.Tuple) -> np.ndarray:
    hit_time = np.full(var.shape, False, dtype=np.bool_)
    hit_time[time_selection] = True
    hit_condition = np.full(var.shape, False, dtype=np.bool_)
    hit_condition[apply_selection] = True
    return hit_time & hit_condition


class Action(ABC):
    def __init__(self, parameters: str):
        if parameters:
            self.parameters: typing.Dict[str, typing.Any] = from_json(parameters)
        else:
            self.parameters: typing.Dict[str, typing.Any] = dict()

    @staticmethod
    def from_code(code: str) -> typing.Callable[[str], "Action"]:
        code = code.lower()
        if code == 'contaminate':
            return Contaminate
        elif code == 'calibration':
            return Calibration
        elif code == 'recalibrate':
            return Recalibrate
        elif code == 'flowcorrection':
            return FlowCorrection
        elif code == 'sizecutfix':
            return SizeCutFix
        elif code == 'abnormaldata':
            return AbnormalData
        return Invalidate

    @property
    def needs_prepare(self) -> bool:
        return False

    def prepare(self, root: Dataset, data: Dataset, times: np.ndarray) -> None:
        pass

    @property
    def limit_profile(self) -> bool:
        return False

    @abstractmethod
    def filter_data(self, root: Dataset, data: Dataset) -> bool:
        pass

    @abstractmethod
    def apply(self, root: Dataset, data: Dataset, selection: typing.Union[slice, np.ndarray]) -> None:
        pass


class Invalidate(Action):
    def __init__(self, parameters: str):
        super().__init__(parameters)
        self.to_invalidate = Selection(self.parameters["selection"])

    def filter_data(self, root: Dataset, data: Dataset) -> bool:
        return self.to_invalidate.filter_data(root, data)

    def apply(self, root: Dataset, data: Dataset, time_selection: typing.Union[slice, np.ndarray]) -> None:
        for var, apply_selection in self.to_invalidate.select_data(root, data):
            try:
                fill_value = var._FillValue
            except AttributeError:
                if np.issubdtype(var.dtype, np.floating):
                    fill_value = nan
                else:
                    fill_value = 0
            if isinstance(time_selection, slice) and len(apply_selection) == 0:
                var[time_selection] = fill_value
            else:
                modified = var[:].data
                modified[_conditional_index(var, time_selection, apply_selection)] = fill_value
                var[:] = modified


class _FlaggingAction(Action):
    @property
    def _flag_name(self) -> str:
        raise NotImplementedError

    @property
    def _flag_bit(self) -> typing.Optional[int]:
        return None

    def filter_data(self, _root: Dataset, _data: Dataset) -> bool:
        return True

    def apply(self, _root: Dataset, data: Dataset, time_selection: typing.Union[slice, np.ndarray]) -> None:
        var = data.variables.get('system_flags')
        if var is not None:
            bit = declare_flag(var, self._flag_name, self._flag_bit)
        else:
            bit = self._flag_bit or 0x01
            var = data.createVariable('system_flags', 'u8', ('time',), fill_value=False)
            variable_coordinates(data, var)
            var.coverage_content_type = "physicalMeasurement"
            var.variable_id = "F1"
            variable_flags(var, {bit: self._flag_name})
            var[:] = 0

        if isinstance(time_selection, slice):
            var[time_selection] = np.bitwise_or(var[:].data[time_selection], bit)
        else:
            modified = var[:].data
            modified[time_selection] = np.bitwise_or(modified[time_selection], bit)
            var[:] = modified


class Contaminate(_FlaggingAction):
    @property
    def _flag_name(self) -> str:
        return "data_contamination_mentor_edit"

    @property
    def _flag_bit(self) -> typing.Optional[int]:
        return 0x2

    @property
    def limit_profile(self) -> bool:
        return True


def _transform_selected_data(
        root: Dataset, data: Dataset, time_selection: typing.Union[slice, np.ndarray],
        selection: Selection,
        transform: typing.Callable[[np.ndarray], np.ndarray]
) -> None:
    for var, apply_selection in selection.select_data(root, data):
        if isinstance(time_selection, slice) and len(apply_selection) == 0:
            var[time_selection] = transform(var[:].data[time_selection])
        else:
            modified = var[:].data
            idx = _conditional_index(var, time_selection, apply_selection)
            modified[idx] = transform(modified[idx])
            var[:] = modified


class Calibration(Action):
    def __init__(self, parameters: str):
        super().__init__(parameters)
        self.to_calibrate = Selection(self.parameters["selection"])
        self.calibration = np.polynomial.Polynomial(np.array(self.parameters["calibration"], dtype=np.float64))

    def filter_data(self, root: Dataset, data: Dataset) -> bool:
        return self.to_calibrate.filter_data(root, data)

    def apply(self, root: Dataset, data: Dataset, time_selection: typing.Union[slice, np.ndarray]) -> None:
        _transform_selected_data(root, data, time_selection, self.to_calibrate, self.calibration)


def _apply_recalibration(
        original: np.ndarray,
        reverse_calibration: np.ndarray,
        calibration: np.polynomial.Polynomial
) -> np.ndarray:
    raw_roots = solve_polynomial(reverse_calibration, original, original)
    difference = np.abs(raw_roots.T - original.T).T
    valid_roots = np.invert(np.all(np.isnan(difference), axis=-1))
    preferred = np.nanargmin(difference[valid_roots], axis=-1)
    raw = np.full_like(original, nan)
    raw[valid_roots] = np.choose(preferred, raw_roots[valid_roots].T).T
    return calibration(raw)


class Recalibrate(Action):
    def __init__(self, parameters: str):
        super().__init__(parameters)
        self.to_calibrate = Selection(self.parameters["selection"])
        self.calibration = np.polynomial.Polynomial(self.parameters["calibration"])
        self.reverse_calibration = np.array(self.parameters["reverse_calibration"], dtype=np.float64)

    def filter_data(self, root: Dataset, data: Dataset) -> bool:
        return self.to_calibrate.filter_data(root, data)

    def _inner(self, original: np.ndarray) -> np.ndarray:
        return _apply_recalibration(original, self.reverse_calibration, self.calibration)

    def apply(self, root: Dataset, data: Dataset, time_selection: typing.Union[slice, np.ndarray]) -> None:
        _transform_selected_data(root, data, time_selection, self.to_calibrate, self._inner)


class FlowCorrection(Action):
    def __init__(self, parameters: str):
        super().__init__(parameters)
        self.instrument = str(self.parameters["instrument"])
        self.calibration = np.polynomial.Polynomial(self.parameters["calibration"])
        self.reverse_calibration = np.array(self.parameters["reverse_calibration"], dtype=np.float64)
        self._flow_ratio: typing.Optional[np.ndarray] = None

    def filter_data(self, root: Dataset, _data: Dataset) -> bool:
        return getattr(root, 'instrument_id', None) == self.instrument

    @property
    def needs_prepare(self) -> bool:
        return True

    def prepare(self, _root: Dataset, data: Dataset, _times: np.ndarray) -> None:
        self._flow_ratio = None

        sample_flow = data.variables.get('sample_flow')
        if sample_flow is None:
            return
        sample_flow = sample_flow[:].data
        if len(sample_flow.shape) == 0:
            sample_flow = sample_flow.reshape((1,))

        ratio = _apply_recalibration(sample_flow, self.reverse_calibration, self.calibration)
        valid = np.all((
            ratio != 0.0,
            sample_flow != 0.0,
        ), axis=0)
        ratio[valid] /= sample_flow[valid]
        ratio[np.invert(valid)] = nan
        self._flow_ratio = ratio

    _NAME_OP: typing.Dict[str, typing.Callable[[np.ndarray, np.ndarray], np.ndarray]] = {
        "number_concentration": lambda original, ratio: original / ratio,
        "scattering_coefficient": lambda original, ratio: original / ratio,
        "backscattering_coefficient": lambda original, ratio: original / ratio,
        "light_absorption": lambda original, ratio: original / ratio,
        "light_extinction": lambda original, ratio: original / ratio,
        "equivalent_black_carbon": lambda original, ratio: original / ratio,
        "number_distribution": lambda original, ratio: original / ratio,
        "normalized_number_distribution": lambda original, ratio: original / ratio,
        "polar_scattering_coefficient": lambda original, ratio: original / ratio,
        "mass_concentration": lambda original, ratio: original / ratio,

        "sample_flow": lambda original, ratio: original * ratio,
        "path_length_change": lambda original, ratio: original * ratio,
    }
    _STANDARD_NAME_OP: typing.Dict[str, typing.Callable[[np.ndarray, np.ndarray], np.ndarray]] = {
        "number_concentration_of_ambient_aerosol_particles_in_air": lambda original, ratio: original / ratio,
        "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles": lambda original, ratio: original / ratio,
        "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles": lambda original, ratio: original / ratio,
        "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles": lambda original, ratio: original / ratio,
        "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles": lambda original, ratio: original / ratio,
    }

    def apply(self, _root: Dataset, data: Dataset, time_selection: typing.Union[slice, np.ndarray]) -> None:
        for name, var in data.variables.items():
            op = self._NAME_OP.get(name)
            if op is None:
                try:
                    op = self._STANDARD_NAME_OP.get(var.standard_name)
                except AttributeError:
                    pass

            if op is None:
                continue

            if isinstance(time_selection, slice):
                if self._flow_ratio is not None:
                    var[time_selection] = op(var[:].data[time_selection], self._flow_ratio[time_selection])
                else:
                    var[time_selection] = nan
            else:
                modified = var[:].data
                if self._flow_ratio is not None:
                    modified[time_selection] = op(modified[time_selection], self._flow_ratio[time_selection])
                else:
                    modified[time_selection] = nan
                var[:] = modified


class SizeCutFix(Action):
    def __init__(self, parameters: str):
        super().__init__(parameters)
        self.operate_size = self.parameters["cutsize"]
        if self.operate_size is not None:
            self.operate_size = float(self.operate_size)
        else:
            self.operate_size = nan

        self.output_cut = self.parameters["modified_cutsize"]
        if self.output_cut == "invalidate":
            self.output_cut: typing.Optional[float] = None
        elif self.operate_size is not None:
            self.output_cut: typing.Optional[float] = float(self.output_cut)
        else:
            self.output_cut: typing.Optional[float] = nan

        self._apply_mask: typing.Optional[np.ndarray] = None
        self._apply_times: bool = True

    @property
    def limit_profile(self) -> bool:
        return True

    def filter_data(self, _root: Dataset, data: Dataset) -> bool:
        return 'cut_size' in data.variables

    @property
    def needs_prepare(self) -> bool:
        return True

    def prepare(self, _root: Dataset, data: Dataset, _times: np.ndarray) -> None:
        cut_size = data.variables['cut_size']
        if not isfinite(self.operate_size):
            self._apply_mask = np.invert(np.isfinite(cut_size[:].data))
        else:
            self._apply_mask = cut_size[:].data == self.operate_size

        self._apply_times = len(cut_size.dimensions) == 1 and cut_size.dimensions[0] == 'time'

    def _apply_invalidate(self, data: Dataset, time_selection: typing.Union[slice, np.ndarray]) -> None:
        for var in data.variables.values():
            cut_dimension = None
            try:
                cut_dimension = var.dimensions.index('cut_size')
            except ValueError:
                pass

            if cut_dimension is not None:
                if self._apply_times:
                    _LOGGER.warning("Cut size dimension detected in variable (%s) with time dependent selection, no size change possible", var.name)
                    continue
                apply_selection = tuple(
                    [time_selection] +
                    ([slice(None)] * (cut_dimension-1)) +
                    [self._apply_mask]
                )
            else:
                if 'cut_size' not in getattr(var, 'ancillary_variables', "").split():
                    continue

                apply_selection = np.full(var.shape, False, dtype=bool)
                apply_selection[time_selection] = True
                apply_selection[np.invert(self._apply_mask)] = False

            try:
                fill_value = var._FillValue
            except AttributeError:
                if np.issubdtype(var.dtype, np.floating):
                    fill_value = nan
                else:
                    fill_value = 0
            modified = var[:].data
            modified[apply_selection] = fill_value
            var[:] = modified

    def apply(self, _root: Dataset, data: Dataset, time_selection: typing.Union[slice, np.ndarray]) -> None:
        if self.output_cut is None:
            return self._apply_invalidate(data, time_selection)

        cut_size = data.variables['cut_size']
        if 'time' in cut_size.dimensions:
            apply_selection = np.full(cut_size.shape, False, dtype=bool)
            apply_selection[time_selection] = True
            apply_selection[np.invert(self._apply_mask)] = False

            modified = cut_size[:].data
            modified[apply_selection] = self.output_cut
            cut_size[:] = modified
        else:
            cut_size[self._apply_mask] = self.output_cut


class AbnormalData(_FlaggingAction):
    _FLAG_MAP = {
        'wild_fire': 'abnormal_data_wild_fire',
        'dust': 'abnormal_data_dust',
    }

    def __init__(self, parameters: str):
        super().__init__(parameters)
        self.set_flag = self._FLAG_MAP[self.parameters['episode_type']]

    @property
    def _flag_name(self) -> str:
        return self.set_flag
