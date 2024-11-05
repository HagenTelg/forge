import typing
from abc import ABC, abstractmethod
import numpy as np
from math import nan, isfinite
from netCDF4 import Dataset, Group, Variable, EnumType
from forge.timeparse import parse_iso8601_duration
from forge.data.structure.timeseries import time_coordinate, cutsize_coordinate, averaged_time_variable, averaged_count_variable
from forge.data.attrs import cell_methods, copy as copy_attrs
from forge.data.state import is_state_group
from forge.data.values import create_and_copy_variable
from .calculate import FileAverager
from . import STANDARD_QUANTILES


def _has_any_time(group: Group) -> bool:
    if 'time' in group.variables:
        return True
    for s in group.groups.values():
        if _has_any_time(s):
            return True
    return False


def _is_metadata_group(group: Group) -> bool:
    if group.name == 'instrument':
        return True
    parent = group.parent
    if parent is not None:
        return _is_metadata_group(parent)
    return False


def _exclude_sub_group(group: Group) -> bool:
    if group.name == 'statistics':
        return True
    if is_state_group(group):
        return True
    if not _has_any_time(group):
        if _is_metadata_group(group):
            return False
        return True
    return False


def _exclude_averaged_variable(variable: Variable) -> bool:
    if variable.name == 'averaged_time':
        return True
    if variable.name == 'averaged_count':
        return True
    return False


class _AverageController(ABC):
    @staticmethod
    def declare_variable(
            input_variable: Variable, output_root: Dataset,
            dimensions_start: typing.Tuple[str] = (),
            dimensions_end: typing.Tuple[str] = (),
            dtype=None,
            copy_attrs: typing.Optional[typing.Iterable[str]] = None,
            fill_value=None
    ) -> Variable:
        if dtype is None:
            if isinstance(input_variable.datatype, EnumType):
                dtype = output_root.enumtypes[input_variable.datatype.name]
            else:
                dtype = input_variable.dtype
        if fill_value is None:
            fill_value = False
            try:
                fill_value = input_variable._FillValue
            except AttributeError:
                pass
        output_dimensions = ('time', *dimensions_start, *input_variable.dimensions[1:], *dimensions_end)
        output_variable = output_root.createVariable(
            input_variable.name, dtype, output_dimensions,
            fill_value=fill_value,
        )
        if copy_attrs is None:
            copy_attrs = input_variable.ncattrs()
        for attr in copy_attrs:
            if attr.startswith('_'):
                continue
            try:
                value = input_variable.getncattr(attr)
            except AttributeError:
                continue
            if attr == 'ancillary_variables' and 'cut_size' in output_dimensions:
                ancillary_variables = set(str(value).split())
                ancillary_variables.discard('cut_size')
                if ancillary_variables:
                    output_variable.setncattr(attr, " ".join(sorted(ancillary_variables)))
            else:
                output_variable.setncattr(attr, value)
        return output_variable

    @staticmethod
    def statistics_category(output_root: Dataset, category: str) -> Group:
        statistics_group = output_root.groups.get("statistics")
        if statistics_group is None:
            statistics_group = output_root.createGroup("statistics")
        output_category = statistics_group.groups.get(category)
        if output_category is None:
            output_category = statistics_group.createGroup(category)
        return output_category

    @staticmethod
    def declare_output(input_variable: Variable, output_root: Dataset, dimensions_start: typing.Tuple[str]) -> Variable:
        return _AverageController.declare_variable(input_variable, output_root, dimensions_start)

    @staticmethod
    def declare_valid_count(input_variable: Variable, output_root: Dataset, dimensions_start: typing.Tuple[str]) -> Variable:
        output_variable = _AverageController.declare_variable(
            input_variable, _AverageController.statistics_category(output_root, 'valid_count'), dimensions_start,
            dtype="u4", copy_attrs=('coordinates', ), fill_value=False,
        )
        output_variable.long_name = "number of data points in the average"
        output_variable.coverage_content_type = "auxillaryInformation"
        output_variable.cell_methods = "time: sum"
        # try:
        #     output_variable.variable_id = input_variable.variable_id + "N"
        # except AttributeError:
        #     pass
        return output_variable

    @staticmethod
    def declare_unweighted_mean(input_variable: Variable, output_root: Dataset, dimensions_start: typing.Tuple[str]) -> Variable:
        output_variable = _AverageController.declare_variable(
            input_variable, _AverageController.statistics_category(output_root, 'unweighted_mean'), dimensions_start,
            copy_attrs=('coordinates', 'units', 'C_format', 'coverage_content_type'),
        )
        output_variable.long_name = "mean value without time coverage information applied"
        output_variable.cell_methods = "time: mean"
        return output_variable

    @staticmethod
    def declare_stddev(input_variable: Variable, output_root: Dataset, dimensions_start: typing.Tuple[str]) -> Variable:
        output_variable = _AverageController.declare_variable(
            input_variable, _AverageController.statistics_category(output_root, 'stddev'), dimensions_start,
            copy_attrs=('coordinates', 'units', 'C_format'),
        )
        output_variable.long_name = "standard deviation in the average period"
        output_variable.coverage_content_type = "auxillaryInformation"
        output_variable.cell_methods = "time: standard_deviation"
        # try:
        #     output_variable.variable_id = input_variable.variable_id + "g"
        # except AttributeError:
        #     pass
        return output_variable

    @staticmethod
    def declare_quantiles(input_variable: Variable, output_root: Dataset, dimensions_start: typing.Tuple[str]) -> Variable:
        category = _AverageController.statistics_category(output_root, 'quantiles')
        if category.dimensions.get('quantile') is None:
            category.createDimension('quantile', len(STANDARD_QUANTILES))
            quantile = category.createVariable('quantile', 'f8', ('quantile', ),
                                               fill_value=nan)
            quantile.long_name = "quantile fraction (0=minimum value, 1=maximum value)"
            quantile.units = "1"
            quantile.C_format = "%7.5f"
            quantile.coverage_content_type = "coordinate"
            quantile[:] = STANDARD_QUANTILES

        output_variable = _AverageController.declare_variable(
            input_variable, category, dimensions_start,
            dimensions_end=('quantile', ),
            copy_attrs=('coordinates', 'units', 'C_format'),
        )
        output_variable.long_name = "quantile values in the average period"
        output_variable.coverage_content_type = "auxillaryInformation"
        return output_variable

    @abstractmethod
    def declare(self, output_root: Dataset, dimensions_start: typing.Tuple[str] = None):
        pass

    @abstractmethod
    def apply(self, averager: FileAverager, values: np.ndarray,
              output_selector: typing.Tuple = None, mask: np.ndarray = None) -> None:
        pass

    @staticmethod
    def create(input_root: Dataset, input_variable: Variable) -> typing.Optional["_AverageController"]:
        if len(input_variable.dimensions) == 0 or input_variable.dimensions[0] != 'time':
            return None

        if input_variable.name == 'system_flags':
            if isinstance(input_variable.datatype, EnumType) or not np.issubdtype(input_variable.dtype, np.integer):
                return None
            return _AverageFlags(input_variable)
        try:
            if input_variable.standard_name == 'status_flag':
                if isinstance(input_variable.datatype, EnumType) or not np.issubdtype(input_variable.dtype, np.integer):
                    return None
                return _AverageFlags(input_variable)
        except AttributeError:
            pass

        methods = cell_methods(input_variable)
        time_method = methods.get('time')
        if time_method == 'last':
            return _AverageLastValid(input_variable)
        elif time_method == 'point' or time_method == 'first':
            return _AverageFirstValid(input_variable)

        if isinstance(input_variable.datatype, EnumType):
            return None

        if time_method == 'sum':
            return _AverageSum(input_variable)

        if not np.issubdtype(input_variable.dtype, np.floating):
            return None

        for check_var, check_method in methods.items():
            if check_method == 'vector_magnitude':
                input_magnitude = input_root.variables.get(check_var)
                if input_magnitude is not None:
                    magnitude_methods = cell_methods(input_magnitude)
                    if magnitude_methods.get(input_variable.name) == 'vector_direction':
                        # Handled by the magnitude
                        return None
            elif check_method == 'vector_direction':
                input_direction = input_root.variables.get(check_var)
                if input_direction is not None:
                    direction_methods = cell_methods(input_direction)
                    if direction_methods.get(input_variable.name) == 'vector_magnitude':
                        return _AverageVector(input_variable, input_direction)

        return _AverageSimple(input_variable)


class _AverageSimple(_AverageController):
    def __init__(self, input_variable: Variable):
        self._input_variable = input_variable

    def declare(self, output_root: Dataset, dimensions_start: typing.Tuple[str] = None):
        if dimensions_start is None:
            dimensions_start = tuple()
        self._output_variable = self.declare_output(self._input_variable, output_root, dimensions_start)
        self._output_valid_count = self.declare_valid_count(self._input_variable, output_root, dimensions_start)
        self._output_unweighted_mean = self.declare_unweighted_mean(self._input_variable, output_root, dimensions_start)
        self._output_stddev = self.declare_stddev(self._input_variable, output_root, dimensions_start)
        if self._input_variable.name != 'quantile' and 'quantile' not in self._input_variable.dimensions:
            self._output_quantiles = self.declare_quantiles(self._input_variable, output_root, dimensions_start)
        else:
            self._output_quantiles = None

    def apply(self, averager: FileAverager, values: np.ndarray,
              output_selector: typing.Tuple = None, mask: np.ndarray = None) -> None:
        if output_selector is None:
            output_selector = (slice(None), )
        self._output_variable[output_selector] = averager(values, mask=mask)
        self._output_valid_count[output_selector] = averager.valid_count(values, mask=mask)
        unweighted_mean = averager.unweighted_mean(values, mask=mask)
        self._output_unweighted_mean[output_selector] = unweighted_mean
        self._output_stddev[output_selector] = averager.stddev(values, unweighted_mean, mask=mask)
        if self._output_quantiles is not None:
            self._output_quantiles[output_selector] = averager.quantiles(values, STANDARD_QUANTILES)


class _AverageFlags(_AverageController):
    def __init__(self, input_variable: Variable):
        self._input_variable = input_variable

    def declare(self, output_root: Dataset, dimensions_start: typing.Tuple[str] = None):
        if dimensions_start is None:
            dimensions_start = tuple()
        self._output_variable = self.declare_output(self._input_variable, output_root, dimensions_start)
        self._output_valid_count = self.declare_valid_count(self._input_variable, output_root, dimensions_start)

    def apply(self, averager: FileAverager, values: np.ndarray,
              output_selector: typing.Tuple = None, mask: np.ndarray = None) -> None:
        if output_selector is None:
            output_selector = (slice(None), )
        self._output_variable[output_selector] = averager.bitwise_or(values)
        self._output_valid_count[output_selector] = averager.valid_count(values, mask=mask)


class _AverageLastValid(_AverageController):
    def __init__(self, input_variable: Variable):
        self._input_variable = input_variable

    def declare(self, output_root: Dataset, dimensions_start: typing.Tuple[str] = None):
        if dimensions_start is None:
            dimensions_start = tuple()
        self._output_variable = self.declare_output(self._input_variable, output_root, dimensions_start)
        self._output_valid_count = self.declare_valid_count(self._input_variable, output_root, dimensions_start)

    def apply(self, averager: FileAverager, values: np.ndarray,
              output_selector: typing.Tuple = None, mask: np.ndarray = None) -> None:
        if output_selector is None:
            output_selector = (slice(None), )
        self._output_variable[output_selector] = averager.last_valid(values, mask=mask)
        self._output_valid_count[output_selector] = averager.valid_count(values, mask=mask)


class _AverageFirstValid(_AverageController):
    def __init__(self, input_variable: Variable):
        self._input_variable = input_variable

    def declare(self, output_root: Dataset, dimensions_start: typing.Tuple[str] = None):
        if dimensions_start is None:
            dimensions_start = tuple()
        self._output_variable = self.declare_output(self._input_variable, output_root, dimensions_start)
        self._output_valid_count = self.declare_valid_count(self._input_variable, output_root, dimensions_start)

    def apply(self, averager: FileAverager, values: np.ndarray,
              output_selector: typing.Tuple = None, mask: np.ndarray = None) -> None:
        if output_selector is None:
            output_selector = (slice(None), )
        self._output_variable[output_selector] = averager.first_valid(values, mask=mask)
        self._output_valid_count[output_selector] = averager.valid_count(values, mask=mask)


class _AverageSum(_AverageController):
    def __init__(self, input_variable: Variable):
        self._input_variable = input_variable

    def declare(self, output_root: Dataset, dimensions_start: typing.Tuple[str] = None):
        if dimensions_start is None:
            dimensions_start = tuple()
        self._output_variable = self.declare_output(self._input_variable, output_root, dimensions_start)
        self._output_valid_count = self.declare_valid_count(self._input_variable, output_root, dimensions_start)

    def apply(self, averager: FileAverager, values: np.ndarray,
              output_selector: typing.Tuple = None, mask: np.ndarray = None) -> None:
        if output_selector is None:
            output_selector = (slice(None), )
        self._output_variable[output_selector] = averager.sum(values, mask=mask)
        self._output_valid_count[output_selector] = averager.valid_count(values, mask=mask)


class _AverageVector(_AverageController):
    def __init__(self, input_magnitude: Variable, input_direction: Variable):
        self._input_magnitude = input_magnitude
        self._input_direction = input_direction
    @staticmethod
    def declare_stability_factor(input_variable: Variable, output_root: Dataset,
                                 dimensions_start: typing.Tuple[str]) -> Variable:
        output_variable = _AverageController.declare_variable(
            input_variable, _AverageController.statistics_category(output_root, 'stability_factor'), dimensions_start,
            copy_attrs=('coordinates', ),
        )
        output_variable.long_name = "ratio of the vector averaged magnitude divided by the arithmetic mean"
        output_variable.units = "1"
        output_variable.C_format = "%4.2f"
        output_variable.coverage_content_type = "auxillaryInformation"
        return output_variable

    def declare(self, output_root: Dataset, dimensions_start: typing.Tuple[str] = None):
        if dimensions_start is None:
            dimensions_start = tuple()

        self._output_magnitude = self.declare_output(self._input_magnitude, output_root, dimensions_start)
        self._output_magnitude_valid_count = self.declare_valid_count(self._input_magnitude, output_root, dimensions_start)
        self._output_magnitude_unweighted_mean = self.declare_unweighted_mean(self._input_magnitude, output_root, dimensions_start)
        self._output_magnitude_stddev = self.declare_stddev(self._input_magnitude, output_root, dimensions_start)
        self._output_magnitude_quantiles = self.declare_quantiles(self._input_magnitude, output_root, dimensions_start)
        self._output_magnitude_stability = self.declare_stability_factor(self._input_magnitude, output_root, dimensions_start)

        self._output_direction = self.declare_output(self._input_direction, output_root, dimensions_start)

    def apply(self, averager: FileAverager, magnitude: np.ndarray,
              output_selector: typing.Tuple = None, mask: np.ndarray = None) -> None:
        if output_selector is None:
            output_selector = (slice(None), )

        direction = self._input_direction[:].data
        direction[np.invert(np.isfinite(magnitude))] = nan
        if mask is not None:
            direction[mask] = nan

        vector_magnitude, vector_direction = averager.vector(magnitude, direction, mask=mask)
        self._output_magnitude[output_selector] = vector_magnitude
        self._output_direction[output_selector] = vector_direction

        self._output_magnitude_valid_count[output_selector] = averager.valid_count(magnitude, mask=mask)
        unweighted_mean = averager.unweighted_mean(magnitude, mask=mask)
        self._output_magnitude_unweighted_mean[output_selector] = unweighted_mean
        self._output_magnitude_stddev[output_selector] = averager.stddev(magnitude, unweighted_mean, mask=mask)
        self._output_magnitude_quantiles[output_selector] = averager.quantiles(magnitude, STANDARD_QUANTILES)

        mean_magnitude = averager(magnitude)
        mean_magnitude[mean_magnitude == 0.0] = nan
        self._output_magnitude_stability[output_selector] = vector_magnitude / mean_magnitude


def _average_data(
        input_root: Dataset,
        output_root: Dataset,
        make_averager: typing.Callable[[np.ndarray, typing.Optional[np.ndarray], typing.Optional[typing.Union[int, float]]], FileAverager],
        time_coverage_resolution: typing.Optional[typing.Union[int, float]] = None,
) -> None:
    if 'time' not in input_root.dimensions:
        return
    times_epoch_ms = input_root.variables['time'][...].data
    if times_epoch_ms.shape[0] == 0:
        # No data, so just eliminate everything
        return

    averaged_time_ms = input_root.variables.get('averaged_time')
    if averaged_time_ms is not None:
        averaged_time_ms = averaged_time_ms[...].data
    averager = make_averager(times_epoch_ms, averaged_time_ms, time_coverage_resolution)

    output_time = time_coordinate(output_root)
    output_time[:] = averager.times

    cut_size = input_root.variables.get('cut_size')
    if cut_size is None or len(cut_size.dimensions) == 0 or cut_size.dimensions[0] != 'time':
        # No cut size or already collapsed into a dimension, so we can treat everything simply
        for name, input_variable in input_root.variables.items():
            if _exclude_averaged_variable(input_variable):
                continue
            controller = _AverageController.create(input_root, input_variable)
            if not controller:
                continue
            controller.declare(output_root)
            controller.apply(averager, input_variable[:].data)
    else:
        cut_size = cut_size[...].data
        finite_sizes = np.isfinite(cut_size)
        possible_sizes = sorted(np.unique(cut_size[finite_sizes]))
        if not np.all(finite_sizes):
            possible_sizes.append(nan)
        output_cut_size = cutsize_coordinate(output_root, len(possible_sizes))
        output_cut_size[:] = possible_sizes

        cut_variables: typing.List[typing.Tuple[Variable, _AverageController]] = list()
        for name, input_variable in input_root.variables.items():
            if _exclude_averaged_variable(input_variable):
                continue
            if name == 'cut_size':
                continue
            if 'cut_size' not in getattr(input_variable, 'ancillary_variables', "").split():
                controller = _AverageController.create(input_root, input_variable)
                if not controller:
                    continue
                controller.declare(output_root)
                controller.apply(averager, input_variable[:].data)
                continue
            controller = _AverageController.create(input_root, input_variable)
            if not controller:
                continue
            controller.declare(output_root, ('cut_size', ))
            cut_variables.append((input_variable, controller))

        for select_size in possible_sizes:
            if isfinite(select_size):
                remove_selector = np.invert(cut_size == select_size)
                output_idx = possible_sizes.index(select_size)
            else:
                remove_selector = np.isfinite(cut_size)
                output_idx = len(possible_sizes) - 1

            for input_variable, controller in cut_variables:
                filtered_data = input_variable[:].data
                if np.issubdtype(filtered_data.dtype, np.floating):
                    filtered_data[remove_selector] = nan
                    mask = None
                else:
                    mask = remove_selector
                    try:
                        fill_value = input_variable._FillValue
                    except AttributeError:
                        fill_value = 0
                    filtered_data[remove_selector] = fill_value
                controller.apply(averager, filtered_data, (slice(None), output_idx), mask)

    if averaged_time_ms is not None:
        averaged_time = averaged_time_variable(output_root)
        averaged_time[:] = averager.averaged_time_ms

    averaged_count = averaged_count_variable(output_root)
    averaged_count[:] = averager.averaged_count


def _average_group(
        input_root: Dataset,
        output_root: Dataset,
        make_averager: typing.Callable[[np.ndarray, typing.Optional[np.ndarray], typing.Optional[typing.Union[int, float]]], FileAverager],
        time_coverage_resolution: typing.Optional[typing.Union[int, float]] = None,
) -> None:
    copy_attrs(input_root, output_root)
    for name, input_dimension in input_root.dimensions.items():
        if name == 'time':
            continue
        output_root.createDimension(name, input_dimension.size)
    for name, input_enum in input_root.enumtypes.items():
        output_root.createEnumType(name, input_enum.dtype, input_enum.enum_dict)
    for name, input_variable in input_root.variables.items():
        if 'time' in input_variable.dimensions:
            continue
        create_and_copy_variable(input_variable, output_root)

    _average_data(input_root, output_root, make_averager, time_coverage_resolution)

    for name, input_group in input_root.groups.items():
        if _exclude_sub_group(input_group):
            continue
        output_group = output_root.createGroup(name)
        _average_group(input_group, output_group, make_averager, time_coverage_resolution)


def average_file(
        input_root: Dataset,
        output_root: Dataset,
        make_averager: typing.Callable[[np.ndarray, typing.Optional[np.ndarray], typing.Optional[typing.Union[int, float]]], FileAverager]
) -> None:
    time_coverage_resolution = getattr(input_root, "time_coverage_resolution", None)
    if time_coverage_resolution is not None:
        try:
            time_coverage_resolution = parse_iso8601_duration(str(time_coverage_resolution))
        except ValueError:
            time_coverage_resolution = None

    _average_group(input_root, output_root, make_averager, time_coverage_resolution)
