import typing
import asyncio
import logging
import time
import enum
import shutil
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
import forge.data.structure.stp as netcdf_stp
from collections import deque
from math import floor, nan
from pathlib import Path
from secrets import token_bytes
from base64 import b32encode
from netCDF4 import Dataset, Variable, Group
from forge.const import __short_version__
from forge.tasks import wait_cancelable
from forge.formattime import format_iso8601_time
from forge.data.structure import instrument_timeseries
from forge.data.structure.history import append_history
from forge.acquisition import LayeredConfiguration
from forge.acquisition.util import parse_interval, write_replace_file
from forge.acquisition.instrument.base import BaseDataOutput


_LOGGER = logging.getLogger(__name__)


def _configure_variable(var: Variable, source: BaseDataOutput.Field) -> None:
    ancillary_variables: typing.List[str] = list()

    group = var.group()

    def _measurement():
        netcdf_timeseries.variable_coordinates(group, var)
        var.coverage_content_type = "physicalMeasurement"

        if 'time' in var.dimensions:
            var.cell_methods = "time: mean"

        if source.use_standard_temperature and group.variables.get("standard_temperature") is not None:
            ancillary_variables.append("standard_temperature")
        if source.use_standard_pressure and group.variables.get("standard_temperature") is not None:
            ancillary_variables.append("standard_pressure")
        if source.use_cut_size and group.variables.get("cut_size") is not None:
            ancillary_variables.append("cut_size")

    if source.template == BaseDataOutput.Field.Template.NONE:
        pass
    elif source.template == BaseDataOutput.Field.Template.METADATA:
        var.coverage_content_type = "referenceInformation"
    elif source.template == BaseDataOutput.Field.Template.STATE:
        netcdf_timeseries.variable_coordinates(group, var)
        var.coverage_content_type = "auxiliaryInformation"
        if 'time' in var.dimensions:
            var.cell_methods = "time: point"
    elif source.template == BaseDataOutput.Field.Template.CUT_SIZE:
        netcdf_timeseries.variable_coordinates(group, var)
        netcdf_var.variable_cutsize(var)
        var.coverage_content_type = "referenceInformation"  # Not measured, so reference is a bit better fit
    elif source.template == BaseDataOutput.Field.Template.DIMENSION:
        var.coverage_content_type = "coordinate"
        if 'time' in var.dimensions:
            var.cell_methods = "time: point"
    elif source.template == BaseDataOutput.Field.Template.MEASUREMENT:
        _measurement()
    elif source.template == BaseDataOutput.Field.Template.STATE_MEASUREMENT:
        _measurement()
        if 'time' in var.dimensions:
            var.cell_methods = "time: point"
    else:
        raise ValueError("invalid variable template type")

    if source.configure_variable:
        source.configure_variable(var)

    if ancillary_variables:
        var.ancillary_variables = " ".join(ancillary_variables)

    for key, value in source.attributes.items():
        if value is None:
            if key in var.ncattrs():
                var.delncattr(key)
            continue
        var.setncattr(key, value)


def _array_value_shape(value: typing.Collection) -> typing.List[int]:
    try:
        iter(value)
        dimension_length = len(value)
    except TypeError:
        return []
    result = [dimension_length]
    for dim in value:
        dim_shape = _array_value_shape(dim)
        if not dim_shape:
            continue
        while len(dim_shape) >= len(result):
            result.append(0)
        for i in range(len(dim_shape)):
            result[i+1] = max(result[i+1], dim_shape[i])
    return result


def _array_value_normalize(value: typing.Collection, shape: typing.List[int],
                           dtype: np.dtype = np.double, pad: typing.Any = nan) -> np.ndarray:
    result = np.full(shape, pad, dtype=dtype)
    try:
        iter(value)
        len(value)
    except TypeError:
        return result

    if len(shape) == 1:
        result[:len(value)] = value
        return result

    index = 0
    for dim in value:
        if index >= shape[0]:
            break
        result[index, ...] = _array_value_normalize(dim, shape[1:], dtype, pad)
        index += 1

    return result


def _write_constants(target: Dataset, constants: typing.List[BaseDataOutput.Field]) -> None:
    for c in constants:
        if isinstance(c, BaseDataOutput.Float):
            constant_value = c.value
            if constant_value is None:
                continue
            var = target.createVariable(c.name, 'f8', fill_value=False)
            _configure_variable(var, c)
            var[0] = float(constant_value)
        elif isinstance(c, BaseDataOutput.Integer):
            constant_value = c.value
            if constant_value is None:
                continue
            var = target.createVariable(c.name, 'i8', fill_value=False)
            _configure_variable(var, c)
            var[0] = int(constant_value)
        elif isinstance(c, BaseDataOutput.UnsignedInteger):
            constant_value = c.value
            if constant_value is None:
                continue
            var = target.createVariable(c.name, 'u8', fill_value=False)
            _configure_variable(var, c)
            var[0] = int(constant_value)
        elif isinstance(c, BaseDataOutput.String):
            constant_value = c.value
            if constant_value is None:
                continue
            var = target.createVariable(c.name, str, fill_value=False)
            _configure_variable(var, c)
            var[0] = str(constant_value)
        elif isinstance(c, BaseDataOutput.ArrayFloat):
            constant_value = c.value
            if not constant_value:
                continue
            constant_shape = _array_value_shape(constant_value)

            constant_dimensions = c.dimensions
            var_dimensions = list()
            if constant_dimensions:
                if len(constant_dimensions) != len(constant_shape):
                    raise ValueError(f"invalid number of dimensions ({len(constant_dimensions)} vs {len(constant_shape)}")

                for dimension_index in range(len(constant_dimensions)):
                    declare_dimension = constant_dimensions[dimension_index]
                    dim = target.dimensions.get(declare_dimension.name)

                    if dim is None:
                        dim = target.createDimension(declare_dimension.name, constant_shape[dimension_index])
                        if isinstance(declare_dimension, BaseDataOutput.ArrayFloat):
                            dvar = target.createVariable(dim.name, 'f8', (dim.name,), fill_value=nan)
                        else:
                            raise ValueError(f"unknown dimension type on {c.field.name}/{dim.name}")
                    else:
                        dvar = target.variables[dim.name]

                    _configure_variable(dvar, declare_dimension)

                    dimension_value = declare_dimension.value

                    if dimension_value:
                        n_assign = min(constant_shape[dimension_index], len(dimension_value))
                        dvar[:n_assign] = dimension_value[:n_assign]

                    var_dimensions.append(dim)
            else:
                for dimension_index in range(len(constant_shape)):
                    dim = target.createDimension(
                        c.name + (str(dimension_index) if dimension_index > 0 else ""),
                        constant_shape[dimension_index]
                    )
                    var_dimensions.append(dim)

            var = target.createVariable(c.name, 'f8', tuple([
                d.name for d in var_dimensions
            ]))
            _configure_variable(var, c)
            var[:] = _array_value_normalize(constant_value, constant_shape)
        else:
            raise ValueError("unknown constant type")


def _configure_record(target: Dataset, record: BaseDataOutput.Record) -> None:
    if record.standard_temperature is not None:
        netcdf_stp.standard_temperature(target, record.standard_temperature)
    if record.standard_pressure is not None:
        netcdf_stp.standard_pressure(target, record.standard_pressure)

    _write_constants(target, record.constants)


class DataOutput(BaseDataOutput):
    def __init__(self, station: str, source: str, config: LayeredConfiguration,
                 working_directory: Path = None,
                 completed_directory: Path = None,
                 average_interval: typing.Optional[float] = None):
        super().__init__(station, source)
        self.config = config
        self._override_config = config.section("METADATA")
        self._average_interval = average_interval

        self.instrument_type: typing.Optional[str] = None

        self._components: typing.List[DataOutput._FileComponent] = list()

        self._automatic_write_task: typing.Optional[asyncio.Task] = None
        self._data_updated: typing.Optional[asyncio.Event] = None

        self._active_output_file: typing.Optional[Path] = None

        self._flush_interval: float = parse_interval(config.get("FLUSH"), 10 * 60)
        if self._flush_interval <= 0.0:
            raise ValueError(f"invalid data flush interval {self._flush_interval}")
        self._file_duration: float = parse_interval(config.get("DURATION"), 60 * 60)
        if self._file_duration <= 0.0:
            raise ValueError(f"invalid data file duration {self._file_duration}")

        if not working_directory:
            working_directory = Path('.')
        self._working_directory: Path = working_directory
        if not completed_directory:
            completed_directory = Path('.')
        self._completed_directory: Path = completed_directory

    class _FileComponent:
        def write_data(self, root: Dataset) -> None:
            raise NotImplementedError

        def advance_file(self):
            pass

        def file_bounds(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float], typing.Optional[float]]:
            return None, None, None

        def average_interval(self) -> typing.Optional[float]:
            return None

    class Record(_FileComponent, BaseDataOutput.Record):
        class _NetCDFVariable:
            def pull_value(self) -> None:
                raise NotImplementedError

            def remove_start(self, count: int) -> None:
                raise NotImplementedError

            def create_variable(self, target: Dataset) -> None:
                raise NotImplementedError

        class _NetCDFVariableFlags(_NetCDFVariable):
            class Bit:
                def __init__(self, source: BaseDataOutput.Flag, bit: int):
                    self.source = source
                    self.bit = bit

            def __init__(self):
                super().__init__()
                self.values = np.empty(0, np.uint64)
                self.bits: typing.List[DataOutput.Record._NetCDFVariableFlags.Bit] = list()
                self.name = "system_flags"
                self._taken_mask: int = 0
                self._bit_names: typing.Dict[int, str] = dict()

            def add_flag(self, source: BaseDataOutput.Flag) -> None:
                selected_bit = 0
                if source.preferred_bit and (self._taken_mask & source.preferred_bit) == 0:
                    selected_bit = source.preferred_bit
                else:
                    for i in range(64):
                        check_bit = 1 << i
                        if (self._taken_mask & check_bit) == 0:
                            selected_bit = check_bit
                            break
                if not selected_bit:
                    raise IndexError
                self.bits.append(self.Bit(source, selected_bit))
                self._taken_mask |= selected_bit
                self._bit_names[selected_bit] = source.name

            def pull_value(self) -> None:
                set_bits: int = 0
                for b in self.bits:
                    if b.source.value:
                        set_bits |= b.bit
                self.values = np.concatenate((self.values, [set_bits]))

            def remove_start(self, count: int) -> None:
                self.values = np.delete(self.values, np.s_[:count])

            def create_variable(self, target: Dataset) -> None:
                var = target.createVariable(self.name, 'u8', ('time',), fill_value=False)
                netcdf_timeseries.variable_coordinates(target, var)
                var.coverage_content_type = "physicalMeasurement"
                var.variable_id = "F1"
                netcdf_var.variable_flags(var, self._bit_names)
                var[:] = self.values

        class _NetCDFVariableNP(_NetCDFVariable):
            def __init__(self, field: typing.Union[BaseDataOutput.Float,
                                                   BaseDataOutput.Integer,
                                                   BaseDataOutput.UnsignedInteger], values: "np.ndarray"):
                super().__init__()
                self.field = field
                self.values = values

            def pull_value(self) -> None:
                v = self.field.value
                if v is None:
                    if np.issubdtype(self.values.dtype, np.floating):
                        v = nan
                    else:
                        v = 0
                # Convoluted because np.append will convert a scalar integer to floating point otherwise
                self.values = np.append(self.values, np.array([v], dtype=self.values.dtype))

            def remove_start(self, count: int) -> None:
                self.values = np.delete(self.values, np.s_[:count])

            def create_variable(self, target: Dataset) -> None:
                fill_value = False
                if np.issubdtype(self.values.dtype, np.floating):
                    fill_value = nan
                var = target.createVariable(self.field.name, self.values.dtype, ('time',), fill_value=fill_value)
                _configure_variable(var, self.field)
                var[:] = self.values

        class _NetCDFVariableNPArray(_NetCDFVariable):
            def __init__(self, field: BaseDataOutput.ArrayFloat, values: "np.ndarray", pad_value):
                super().__init__()
                self.field = field
                self.values = values
                self.pad = pad_value
                self.shapes: typing.Deque[typing.List[int]] = deque()

                field_dimensions = self.field.dimensions
                if field_dimensions:
                    self.values = np.reshape(self.values, tuple([0] * (len(field_dimensions) + 1)))

            def pull_value(self) -> None:
                v = self.field.value
                if v is None:
                    v = []

                add_shape = _array_value_shape(v)
                if add_shape == [0] and len(self.values.shape) > 1:
                    # This happens when bypassed, so just continue with nothing changed
                    add_shape = self.values.shape[1:]

                if len(self.values.shape) != len(add_shape)+1:
                    if self.values.shape[0] != 0:
                        _LOGGER.warning(f"Dimensionality change detected on {self.field.name} from {self.values.shape} to {tuple(add_shape)}, existing data discarded")
                    reshaped = [self.values.shape[0]]
                    reshaped.extend(add_shape)
                    self.values = np.full(reshaped, self.pad, dtype=self.values.dtype)
                    fill_shapes = len(self.shapes)
                    if fill_shapes > 0:
                        self.shapes.clear()
                        for i in range(fill_shapes):
                            self.shapes.append([0] * len(add_shape))

                self.shapes.append(add_shape)

                pad_sizes = [0]
                any_padded = False
                for dimension_index in range(len(add_shape)):
                    n_pad = add_shape[dimension_index] - self.values.shape[dimension_index+1]
                    if n_pad > 0:
                        pad_sizes.append(n_pad)
                        any_padded = True
                    else:
                        pad_sizes.append(0)

                if any_padded:
                    self.values = np.pad(self.values, [(0, s) for s in pad_sizes],
                                         'constant', constant_values=self.pad)

                v = _array_value_normalize(v, self.values.shape[1:], self.values.dtype, self.pad)

                self.values = np.concatenate((self.values, [v]))

            def remove_start(self, count: int) -> None:
                newsize = [0] * (len(self.values.shape) - 1)
                if count >= len(self.shapes):
                    self.shapes.clear()
                else:
                    for i in range(count):
                        self.shapes.popleft()
                    for add_shape in self.shapes:
                        for dimension_index in range(len(add_shape)):
                            if dimension_index >= len(newsize):
                                break
                            newsize[dimension_index] = max(add_shape[dimension_index], newsize[dimension_index])
                self.values = np.delete(self.values, np.s_[:count], 0)
                for dimension_index in range(len(newsize)):
                    dimension_size = newsize[dimension_index]
                    if dimension_size >= self.values.shape[dimension_index+1]:
                        continue
                    self.values = np.delete(self.values, np.s_[dimension_size:], dimension_index+1)

            def create_variable(self, target: Dataset) -> None:
                field_dimensions = self.field.dimensions
                var_dimensions = list()
                if field_dimensions:
                    for dimension_index in range(min(len(field_dimensions), len(self.values.shape) - 1)):
                        declare_dimension = field_dimensions[dimension_index]
                        dimension_value = declare_dimension.value

                        dim = target.dimensions.get(declare_dimension.name)
                        if dim is None:
                            dimension_length = max(self.values.shape[dimension_index+1], 1)
                            if dimension_value:
                                dimension_length = max(dimension_length, len(dimension_value))
                            dim = target.createDimension(declare_dimension.name, dimension_length)
                            if isinstance(declare_dimension, BaseDataOutput.ArrayFloat):
                                dvar = target.createVariable(dim.name, 'f8', (dim.name,), fill_value=nan)
                            else:
                                raise ValueError(f"unknown dimension type on {self.field.name}/{dim.name}")
                        else:
                            dvar = target.variables[dim.name]

                        _configure_variable(dvar, declare_dimension)

                        if dimension_value:
                            n_assign = min(len(dimension_value), dim.size)
                            dvar[:n_assign] = dimension_value[:n_assign]

                        var_dimensions.append(dim)
                else:
                    for dimension_index in range(len(self.values.shape) - 1):
                        dim = target.createDimension(
                            self.field.name + (str(dimension_index) if dimension_index > 0 else ""),
                            max(self.values.shape[dimension_index + 1], 1)
                        )
                        var_dimensions.append(dim)

                dimension_names = ['time'] + [d.name for d in var_dimensions]

                var = target.createVariable(self.field.name, self.values.dtype, tuple(dimension_names),
                                            fill_value=self.pad)
                _configure_variable(var, self.field)

                if self.values.shape[0] == 0:
                    # May not have been shaped into something compatible with the number of dimensions yet
                    return

                assign_indices = [np.s_[:]]
                for dimension_index in range(1, len(self.values.shape)):
                    assign_indices.append(np.s_[:min(self.values.shape[dimension_index],
                                                     var_dimensions[dimension_index-1].size)])
                var[:] = self.values[tuple(assign_indices)]

        class _NetCDFVariableNative(_NetCDFVariable):
            def __init__(self, field: typing.Union[BaseDataOutput.String], data_type):
                super().__init__()
                self.field = field
                self.data_type = data_type
                self.values = deque()

            def pull_value(self) -> None:
                self.values.append(self.field.value)

            def remove_start(self, count: int) -> None:
                for i in range(count):
                    try:
                        self.values.popleft()
                    except IndexError:
                        break

            def create_variable(self, target: Dataset) -> None:
                var = target.createVariable(self.field.name, self.data_type, ('time',), fill_value=False)
                _configure_variable(var, self.field)
                for i in range(len(self.values)):
                    v = self.values[i]
                    if v is None:
                        continue
                    var[i] = v

        class _NetCDFVariableEnum(_NetCDFVariable):
            def __init__(self, field: BaseDataOutput.Enum):
                super().__init__()
                self.field = field
                self.values = deque()

            def pull_value(self) -> None:
                self.values.append(self.field.value)

            def remove_start(self, count: int) -> None:
                for i in range(count):
                    try:
                        self.values.popleft()
                    except IndexError:
                        break

            def _create_string(self, target: Dataset) -> None:
                var = target.createVariable(self.field.name, str, ('time',), fill_value=False)
                _configure_variable(var, self.field)
                for i in range(len(self.values)):
                    v = self.values[i]
                    if v is None:
                        continue
                    var[i] = str(v)

            def create_variable(self, target: Dataset) -> None:
                enum_type = self.field.enum

                value_min = 0
                value_max = 0
                enum_dict: typing.Dict[str, int] = dict()
                default_value: typing.Optional[int] = None
                for t in enum_type:
                    if not isinstance(t.value, int):
                        return self._create_string(target)

                    enum_dict[t.name] = t.value

                    if default_value is None:
                        default_value = t.value

                    if t.value < value_min:
                        value_min = t.value
                    if t.value > value_max:
                        value_max = t.value

                data_type = target.enumtypes.get(self.field.typename)
                if data_type is None:
                    for check_type in (np.uint8, np.int8, np.uint16, np.int16, np.uint32, np.int32, np.uint64):
                        ti = np.iinfo(check_type)
                        if ti.min <= value_min and ti.max >= value_max:
                            base_dtype = check_type
                            break
                    else:
                        base_dtype = np.int64

                    data_type = target.createEnumType(base_dtype, self.field.typename, enum_dict)

                var = target.createVariable(self.field.name, data_type, ('time',), fill_value=False)
                _configure_variable(var, self.field)
                for i in range(len(self.values)):
                    v = self.values[i]
                    if v is None:
                        var[i] = default_value
                    else:
                        var[i] = v

        def __init__(self, output: "DataOutput", name: str):
            DataOutput._FileComponent.__init__(self)
            BaseDataOutput.Record.__init__(self)

            self.output = output
            self.name = name

            self.times = np.empty(0, np.int64)
            self.variables: typing.List[DataOutput.Record._NetCDFVariable] = list()
            self.flags: typing.List[DataOutput.Record._NetCDFVariableFlags] = list()

        def add_variable(self, field: "BaseDataOutput.Field") -> None:
            if isinstance(field, BaseDataOutput.Float):
                self.variables.append(self._NetCDFVariableNP(field, np.empty(0, np.double)))
            elif isinstance(field, BaseDataOutput.Integer):
                self.variables.append(self._NetCDFVariableNP(field, np.empty(0, np.int64)))
            elif isinstance(field, BaseDataOutput.UnsignedInteger):
                self.variables.append(self._NetCDFVariableNP(field, np.empty(0, np.uint64)))
            elif isinstance(field, BaseDataOutput.String):
                self.variables.append(self._NetCDFVariableNative(field, str))
            elif isinstance(field, BaseDataOutput.ArrayFloat):
                self.variables.append(self._NetCDFVariableNPArray(field, np.empty((0, 0), np.double), nan))
            elif isinstance(field, BaseDataOutput.Enum):
                self.variables.append(self._NetCDFVariableEnum(field))
            else:
                raise ValueError(f"unknown field type on {field.name}")

        def add_flag(self, source: "BaseDataOutput.Flag") -> None:
            for f in self.flags:
                try:
                    f.add_flag(source)
                except IndexError:
                    pass
                return

            f = self._NetCDFVariableFlags()
            if len(self.flags) > 0:
                f.name = f.name + str(len(self.flags)+1)
            self.flags.append(f)
            f.add_flag(source)

        def pull_record(self, time: float) -> None:
            self.times = np.append(self.times, round(time * 1000.0))
            for f in self.flags:
                f.pull_value()
            for v in self.variables:
                v.pull_value()

        def start_group(self, root: Dataset) -> Group:
            target = root.createGroup(self.name)
            return target

        def declare_time(self, target: Dataset) -> Variable:
            return netcdf_timeseries.time_coordinate(target)

        def end_group(self, target: Dataset) -> None:
            pass

        def write_data(self, root: Dataset) -> None:
            target = self.start_group(root)
            time_var = self.declare_time(target)
            _configure_record(target, self)

            time_var[:] = self.times
            for f in self.flags:
                f.create_variable(target)
            for v in self.variables:
                v.create_variable(target)

            self.end_group(target)

    class MeasurementRecord(Record, BaseDataOutput.MeasurementRecord):
        def __init__(self, output: "DataOutput", name: str):
            DataOutput.Record.__init__(self, output, name)
            BaseDataOutput.MeasurementRecord.__init__(self)

            self.start_time: typing.Optional[float] = None
            self.end_time: typing.Optional[float] = None
            self.first_time: typing.Optional[float] = None

            self.total_milliseconds = np.empty(0, np.uint64)
            self.total_samples = np.empty(0, np.uint32)

        def advance_file(self):
            n_del = self.times.shape[0]
            self.times = np.delete(self.times, np.s_[:n_del], 0)
            self.total_milliseconds = np.delete(self.total_milliseconds, np.s_[:n_del], 0)
            self.total_samples = np.delete(self.total_samples, np.s_[:n_del], 0)
            self.start_time = None
            self.end_time = None

            for f in self.flags:
                f.remove_start(n_del)
            for v in self.variables:
                v.remove_start(n_del)

        def __call__(self, start_time: float, end_time: float, total_seconds: float, total_samples: int) -> None:
            if self.output._data_updated:
                self.output._data_updated.set()

            if not self.start_time:
                self.start_time = start_time
            self.end_time = end_time

            if not self.first_time:
                self.first_time = start_time

            self.pull_record(start_time)
            self.total_milliseconds = np.append(self.total_milliseconds, round(total_seconds * 1000.0))
            self.total_samples = np.append(self.total_samples, total_samples)

        def end_group(self, target: Dataset) -> None:
            var = netcdf_timeseries.averaged_time_variable(target)
            var[:] = self.total_milliseconds

            var = netcdf_timeseries.averaged_count_variable(target)
            var[:] = self.total_samples

        def file_bounds(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float], typing.Optional[float]]:
            return self.start_time, self.end_time, self.first_time

        def average_interval(self) -> typing.Optional[float]:
            return self.report_interval

    def measurement_record(self, name: str) -> "BaseDataOutput.MeasurementRecord":
        r = self.MeasurementRecord(self, name)
        self._components.append(r)
        return r

    class StateRecord(Record, BaseDataOutput.StateRecord):
        def __init__(self, output: "DataOutput", name: str):
            DataOutput.Record.__init__(self, output, name)
            BaseDataOutput.StateRecord.__init__(self)

        def __call__(self, now: float, historical: bool = False) -> None:
            if self.output._data_updated and not historical:
                self.output._data_updated.set()
            self.pull_record(now)

        def declare_time(self, target: Dataset) -> Variable:
            return netcdf_timeseries.state_change_coordinate(target)

        def advance_file(self):
            n_del = self.times.shape[0] - 1
            if n_del <= 0:
                return
            self.times = np.delete(self.times, np.s_[:n_del], 0)
            for f in self.flags:
                f.remove_start(n_del)
            for v in self.variables:
                v.remove_start(n_del)

    def state_record(self, name: str) -> "BaseDataOutput.StateRecord":
        r = self.StateRecord(self, name)
        self._components.append(r)
        return r

    class ConstantRecord(_FileComponent, BaseDataOutput.ConstantRecord):
        def __init__(self, output: "DataOutput", name: str):
            DataOutput._FileComponent.__init__(self)
            BaseDataOutput.ConstantRecord.__init__(self)

            self.output = output
            self.name = name

        def write_data(self, root: Dataset) -> None:
            target = root.createGroup(self.name)

            if self.standard_temperature is not None:
                netcdf_stp.standard_temperature(target, self.standard_temperature)
            if self.standard_pressure is not None:
                netcdf_stp.standard_pressure(target, self.standard_pressure)

            _write_constants(target, self.constants)

    def constant_record(self, name: str) -> "BaseDataOutput.ConstantRecord":
        r = self.ConstantRecord(self, name)
        self._components.append(r)
        return r

    def _query_override(self, key: str) -> typing.Any:
        return self._override_config.get(key)

    def write_file(self, filename: str) -> None:
        root = Dataset(filename, 'w', format='NETCDF4')

        start_epoch: typing.Optional[float] = None
        end_epoch: typing.Optional[float] = None
        first_epoch: typing.Optional[float] = None
        average_interval: typing.Optional[float] = None
        for c in self._components:
            s, e, f = c.file_bounds()
            if s and (not start_epoch or s < start_epoch):
                start_epoch = s
            if e and (not end_epoch or e > end_epoch):
                end_epoch = e
            if f and (not first_epoch or f < first_epoch):
                first_epoch = f
            i = c.average_interval()
            if i and (not average_interval or i < average_interval):
                average_interval = i

        if not self._average_interval:
            average_interval = None
        elif not average_interval or average_interval < self._average_interval:
            average_interval = self._average_interval

        instrument_timeseries(root, self.station, self.source,
                              start_epoch, end_epoch, average_interval,
                              tags=self.tags, override=self._query_override)

        if first_epoch:
            root.setncattr("acquisition_start_time", format_iso8601_time(first_epoch))

        if self.instrument_type:
            root.setncattr("instrument_vocabulary", f"Forge Acquisition {__short_version__}")
            root.setncattr("instrument", self.instrument_type)
            append_history(root, "forge.acquisition." + self.instrument_type)
        else:
            append_history(root, "forge.acquisition")

        for c in self._components:
            c.write_data(root)

        root.close()

    def _flush_file(self):
        if self._data_updated:
            self._data_updated.clear()

        write_replace_file(str(self._active_output_file), str(self._working_directory), self.write_file)

        _LOGGER.debug("Data flush completed")

    def _set_target_name(self):
        filetime = format_iso8601_time(time.time(), delimited=False)
        uid = b32encode(token_bytes(10)).decode('ascii')
        self._active_output_file = self._working_directory / f"{self.station.upper()}-{self.source}_a{filetime}_u{uid}.nc"
        _LOGGER.info(f"Data output file set to {str(self._active_output_file)}")

    async def _advance_file(self):
        self._flush_file()

        source_file = self._active_output_file
        target_file = self._completed_directory / self._active_output_file.name

        self._set_target_name()
        for c in self._components:
            c.advance_file()

        try:
            await asyncio.get_event_loop().run_in_executor(None, shutil.move,
                                                           str(source_file), str(target_file))
            _LOGGER.debug(f"Moved completed data file {source_file} to {target_file}")
        except OSError:
            _LOGGER.warning(f"Failed to relocate completed data file {source_file} to {target_file}", exc_info=True)

    async def _automatic_write(self) -> None:
        def next_interval(now: float, interval: float) -> float:
            return floor(now / interval) * interval + interval

        now = time.time()
        next_flush = now + 60.0
        next_file = next_interval(now, self._file_duration)
        if next_file < next_flush:
            next_file = next_flush
        flush_skipped = True
        while True:
            maximum_sleep = min(next_flush, next_file) - now
            if maximum_sleep < 0.001:
                maximum_sleep = 0.001
            if flush_skipped:
                try:
                    await wait_cancelable(self._data_updated.wait(), maximum_sleep)
                except asyncio.TimeoutError:
                    pass
            else:
                await asyncio.sleep(maximum_sleep)

            now = time.time()
            if now >= next_file:
                await asyncio.shield(self._advance_file())
                now = time.time()
                next_file = next_interval(now, self._file_duration)
                next_flush = next_interval(now, self._flush_interval)
            elif now >= next_flush:
                if self._data_updated.is_set():
                    flush_skipped = False
                    self._flush_file()
                    now = time.time()
                else:
                    flush_skipped = True
                next_flush = next_interval(now, self._flush_interval)
            elif flush_skipped and self._data_updated.is_set():
                flush_skipped = False
                self._flush_file()
                now = time.time()
                next_flush = next_interval(now, self._flush_interval)

    async def start(self) -> None:
        self._set_target_name()
        self._data_updated = asyncio.Event()
        self._automatic_write_task = asyncio.ensure_future(self._automatic_write())

    async def shutdown(self) -> None:
        if self._automatic_write_task:
            t = self._automatic_write_task
            self._automatic_write_task = None
            try:
                t.cancel()
            except:
                pass
            try:
                await t
            except asyncio.CancelledError:
                pass
            except:
                _LOGGER.warning("Error in automatic file write", exc_info=True)
        self._data_updated = None
        self._flush_file()


if __name__ == '__main__':
    import sys
    target_file = sys.argv[1]
    print("Writing data to", target_file)

    data = DataOutput('bos', 'N61', LayeredConfiguration(), average_interval=60.0)

    class ConstantFloat(BaseDataOutput.Float):
        def __init__(self, name: str, value: float):
            super().__init__(name)
            self._value = value

        @property
        def value(self) -> float:
            return self._value

    class ConstantString(BaseDataOutput.String):
        def __init__(self, name: str, value: str):
            super().__init__(name)
            self._value = value

        @property
        def value(self) -> str:
            return self._value

    class ConstantArrayFloat(BaseDataOutput.ArrayFloat):
        def __init__(self, name: str, value: typing.List[float]):
            super().__init__(name)
            self._value = value
            self.dim: typing.Optional["BaseDataOutput.ArrayFloat"] = None

        @property
        def value(self) -> typing.List[float]:
            return self._value

        @property
        def dimensions(self) -> typing.Optional[typing.List["BaseDataOutput.ArrayFloat"]]:
            return [self.dim]

    class ConstantMatrixFloat(BaseDataOutput.ArrayFloat):
        def __init__(self, name: str, value: typing.List):
            super().__init__(name)
            self._value = value
            self.dim1: typing.Optional["BaseDataOutput.ArrayFloat"] = None
            self.dim2: typing.Optional["BaseDataOutput.ArrayFloat"] = None

        @property
        def value(self) -> typing.List:
            return self._value

        @property
        def dimensions(self) -> typing.Optional[typing.List["BaseDataOutput.ArrayFloat"]]:
            return [self.dim1, self.dim2]

    class ConstantFlag(BaseDataOutput.Flag):
        def __init__(self, name: str, value: bool):
            super().__init__(name)
            self._value = value

        @property
        def value(self) -> bool:
            return self._value

    rec = data.measurement_record('data')
    rec.add_variable(ConstantFloat('var1', 1.0))
    rec.add_variable(ConstantString('var2', "value"))
    rec.add_flag(ConstantFlag("flag1", True))
    rec.add_flag(ConstantFlag("flag2", False))
    af = ConstantArrayFloat('var3', [3.0, 4.0])
    af.dim = ConstantArrayFloat('afdim', [5.0, 6.0])
    rec.add_variable(af)
    mf = ConstantMatrixFloat('var3', [[7.0, 8.0], [9.0, 10.0], [11.0, 13.0]])
    mf.dim1 = ConstantArrayFloat('mfdim1', [14.0, 15.0])
    mf.dim2 = ConstantArrayFloat('mfdim2', [16.0, 17.0, 18.0])
    rec.add_variable(mf)

    rec(1658275200, 1658275260, 60.0, 30)
    rec(1658275260, 1658275320, 59.0, 29)

    rec = data.state_record('state')
    rec.add_variable(ConstantString('sv', "123"))
    rec(1658275200)

    rec = data.constant_record('instrument')
    rec.constants.append(ConstantString('model', "Testing"))
    rec.constants.append(ConstantFloat('fn', 3.0))

    data.write_file(target_file)
