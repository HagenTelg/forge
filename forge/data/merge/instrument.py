import typing
import logging
import netCDF4
import numpy as np
from math import floor, ceil, nan, inf, isfinite
from forge.const import MAX_I64
from forge.range import subtract_tuple
from forge.timeparse import parse_iso8601_time
from forge.formattime import format_iso8601_time
from forge.data.structure.variable import variable_flags, variable_cutsize
from .enum import MergeEnum
from ..state import is_state_group
from ..flags import parse_flags

if typing.TYPE_CHECKING:
    from pathlib import Path

_LOGGER = logging.getLogger(__name__)


def _parse_history(value: typing.Optional[str],
                   limit_start: typing.Optional[int] = None,
                   limit_end: typing.Optional[int] = None) -> typing.Dict[int, str]:
    result: typing.Dict[int, str] = dict()
    if not value:
        return result

    for line in str(value).strip().split('\n'):
        try:
            end_time, contents = line.split(',', 1)
        except ValueError:
            _LOGGER.warning(f"Malformed history line {line}")
            continue
        end_time = int(ceil(parse_iso8601_time(str(end_time)).timestamp() * 1000.0))
        if limit_start and end_time < limit_start:
            continue
        if limit_end and end_time >= limit_end:
            continue
        result[end_time] = contents
    return result


def _reduce_to_slice(indices) -> typing.Union[slice, np.ndarray]:
    for i in range(1, len(indices)):
        if indices[i] != indices[i - 1] + 1:
            break
    else:
        return slice(indices[0], indices[-1] + 1)
    return np.array(indices, dtype=np.uint32)


def _find_dimension(data: netCDF4.Dataset, name: str) -> netCDF4.Dimension:
    while True:
        try:
            return data.dimensions[name]
        except KeyError:
            data = data.parent
            if data is None:
                raise KeyError(f"Dimension {name} not found")


def _wavelength_mapping(wavelength_source: netCDF4.Variable, destination_size: int):
    if destination_size == 0:
        destination_size = 1

    def to_wavelength(wavelength) -> typing.Optional[float]:
        try:
            if wavelength.mask:
                return None
        except AttributeError:
            pass
        wavelength = float(wavelength)
        if not isfinite(wavelength):
            return None
        return wavelength

    def to_index(wavelength) -> typing.Optional[int]:
        if wavelength is None:
            return None
        if destination_size == 3 or destination_size == 4:
            if 400 <= wavelength < 500:
                return 0
            if 500 <= wavelength < 600:
                return 1
            if 600 <= wavelength < 750:
                return 2
        if destination_size == 4:
            if 750 <= wavelength < 900:
                return 3
        return None

    if len(wavelength_source.shape) == 0 or wavelength_source.shape[0] == 1:
        target_index = to_index(to_wavelength(wavelength_source[0]))
        if target_index is None:
            target_index = 0
        return slice(target_index, target_index+1), 1

    assert destination_size >= wavelength_source.shape[0]

    wavelength_order = [
        (to_wavelength(wavelength_source[idx]) or inf, idx)
        for idx in range(wavelength_source.shape[0])
    ]
    wavelength_order.sort(key=lambda x: x[0])

    output_apply: typing.List[int] = [0] * wavelength_source.shape[0]

    remapped_indices: typing.Set[int] = set()
    for wavelength, sidx in wavelength_order:
        target_index = to_index(wavelength)
        if target_index is None:
            break
        if target_index in remapped_indices:
            break
        remapped_indices.add(target_index)
        output_apply[target_index] = sidx
    else:
        return _reduce_to_slice(output_apply), len(wavelength_order)

    for didx in range(len(wavelength_order)):
        output_apply[didx] = wavelength_order[didx][1]
    return _reduce_to_slice(output_apply), len(wavelength_order)


def _cut_size_mapping(cut_size_source: netCDF4.Variable, cut_size_destination: netCDF4.Variable):
    if len(cut_size_destination.shape) != 1 or cut_size_destination.shape[0] == 1:
        return slice(1), 1

    def to_wavelength(value):
        try:
            if value.mask:
                return nan
        except AttributeError:
            pass
        return float(value)

    if not cut_size_source.shape or cut_size_source.shape[0] == 1:
        target_index = int(np.where(cut_size_destination[:] == to_wavelength(cut_size_source[0]))[0])
        return slice(target_index, target_index+1), 1

    output_apply: typing.List[int] = [0] * cut_size_source.shape[0]
    for sidx in range(cut_size_source.shape[0]):
        cut_size = to_wavelength(cut_size_source[sidx])
        didx = int(np.where(cut_size_destination[:] == cut_size)[0])
        output_apply[sidx] = didx
    return _reduce_to_slice(output_apply), len(output_apply)


class _TimeMapping:
    def __init__(self, source_start: int, source_end: int, destination_start: int, destination_end: int):
        self.source_start = source_start
        self.source_end = source_end
        self.destination_start = destination_start
        self.destination_end = destination_end

    @property
    def count(self):
        return self.source_end - self.source_start


class _DataMapping:
    def __init__(
            self,
            source_shape: typing.Tuple[int, ...],
            source_selection: typing.Tuple[slice, ...],
            source_reshape: typing.Optional[typing.Tuple[int, ...]],
            source_transpose: typing.Optional[typing.Tuple[int, ...]],
            source_apply: typing.Tuple[slice, ...],
            destination_apply: typing.Tuple[typing.Union[slice, np.ndarray], ...],
    ):
        self.source_shape = source_shape
        self.source_selection = source_selection
        self.source_reshape = source_reshape
        self.source_transpose = source_transpose
        self.source_apply = source_apply
        self.destination_apply = destination_apply

    def netcdf_assignments(self, source_shape: typing.Tuple[int, ...]):
        # Complicated handling because NetCDF variables do not support advanced indexing, so we need to unroll
        # that case

        def recursive_gen(destination, source):
            next_dimension_index = len(destination)
            if next_dimension_index >= len(self.destination_apply):
                yield tuple(destination), tuple(source)
                return

            next_dest = self.destination_apply[next_dimension_index]
            next_source = self.source_apply[next_dimension_index]
            if isinstance(next_dest, slice):
                yield from recursive_gen(destination + [next_dest], source + [next_source])
                return

            dest_idx = 0
            for source_idx in range(*next_source.indices(source_shape[next_dimension_index])):
                yield from recursive_gen(destination + [next_dest[dest_idx]], source + [source_idx])
                dest_idx += 1

        yield from recursive_gen([], [])


class _HistoryAttribute:
    def __init__(self, name: str):
        self.name = name
        self.latest: typing.Optional[typing.Any] = None
        self.prior: typing.Optional[typing.Any] = None
        self.history: typing.Dict[int, str] = dict()

    def to_history(self, value: typing.Any) -> typing.Any:
        return value

    def format_history(self, value: typing.Any) -> typing.Any:
        return str(value)

    def apply_data(self, start: int, end: int, contents: typing.Union[netCDF4.Dataset, netCDF4.Variable]) -> None:
        value = getattr(contents, self.name, None)
        if value is None:
            return
        self.latest = value

        self.history.update(_parse_history(
            getattr(contents, self.name + '_history', None),
            start, end
        ))

        value = self.to_history(value)
        if self.prior is not None and value != self.prior:
            self.history[start] = self.format_history(self.prior)
        self.prior = value

    def finish(self, target: typing.Union[netCDF4.Dataset, netCDF4.Variable]) -> None:
        if self.latest is None:
            try:
                target.delncattr(self.name)
            except (AttributeError, RuntimeError):
                pass
            try:
                target.delncattr(self.name + '_history')
            except (AttributeError, RuntimeError):
                pass
            return
        target.setncattr(self.name, self.latest)

        if not self.history:
            try:
                target.delncattr(self.name + '_history')
            except (AttributeError, RuntimeError):
                pass
            return

        target.setncattr(self.name + '_history', "\n".join([
            f"{format_iso8601_time(end / 1000.0)},{self.history[end]}"
            for end in sorted(self.history.keys())
        ]))


class _Variable:
    def __init__(self, name: str, record: "_Record"):
        self.name = name
        self.record = record
        self.variable: typing.Optional[netCDF4.Variable] = None
        self.dimensions: typing.List[str] = list()
        self.dtype: typing.Optional = None
        self.fill_value: typing.Optional = None
        self.enum_type: typing.Optional[str] = None
        self.next_time_index: int = 0
        self.last_time_value: typing.Optional[int] = None
        self.ancillary_cut_size: bool = False

    def incorporate_structure(self, contents: netCDF4.Variable, is_state: typing.Optional[bool]) -> None:
        dimensions = contents.dimensions
        if dimensions and dimensions[0] == 'time':
            dimensions = dimensions[1:]

        for dim in dimensions:
            if dim in self.dimensions:
                continue
            self.dimensions.append(dim)

        if not self.dtype:
            self.dtype = contents.dtype
            try:
                self.fill_value = contents._FillValue
            except AttributeError:
                pass
        if self.enum_type is None:
            self.enum_type = contents.datatype.name if isinstance(contents.datatype, netCDF4.EnumType) else ""

        ancillary_variables = getattr(contents, 'ancillary_variables', "").split()
        if 'cut_size' in ancillary_variables:
            self.ancillary_cut_size = True

    def complete_structure(self) -> None:
        pass

    @property
    def time_dependent(self) -> bool:
        return False

    @property
    def is_time_dimension(self) -> bool:
        return False

    @property
    def bind_dimensions(self) -> typing.List[str]:
        dimension_sort: typing.List[typing.Tuple[int, str]] = list()
        for dim in self.dimensions:
            priority = 0
            if dim == 'wavelength':
                priority = 1
            elif dim == 'cut_size':
                priority = -1
            dimension_sort.append((priority, dim))
        dimension_sort.sort(key=lambda x: x[0])

        return [v[1] for v in dimension_sort]

    def declare_structure(self, root: netCDF4.Dataset) -> None:
        if self.ancillary_cut_size and 'cut_size' not in self.dimensions and 'cut_size' in self.record.dimension_size:
            # Constant to split size promotion
            self.dimensions.append('cut_size')

        fill_value = False
        if self.enum_type:
            self.dtype = root.enumtypes[self.enum_type]
        elif self.fill_value is not None:
            try:
                fill_value = np.array(self.fill_value).astype(self.dtype).item()
            except ValueError:
                pass
            if not fill_value and np.issubdtype(self.dtype, np.floating) and self.time_dependent:
                fill_value = nan

        self.variable = root.createVariable(self.name, self.dtype, tuple(self.bind_dimensions), fill_value=fill_value)

    def incorporate_variable(self, contents: netCDF4.Variable) -> None:
        for attr in contents.ncattrs():
            if attr.startswith('_'):
                continue
            if attr in self.variable.ncattrs():
                continue
            self.variable.setncattr(attr, contents.getncattr(attr))

    def apply_constant(self, contents: netCDF4.Variable) -> None:
        pass

    def data_mapping(
            self,
            contents: netCDF4.Variable,
            source: typing.List[netCDF4.Dimension], destination: typing.List[netCDF4.Dimension]
    ) -> _DataMapping:
        missing_destination_dimensions: typing.Dict[str, int] = {
            destination[idx].name: idx for idx in range(len(destination))
        }
        # Destination should have been declared with the maximal set of dimensions, so there is always a target
        source_transpose: typing.List[int] = [
            missing_destination_dimensions.pop(source[sidx].name) for sidx in range(len(source))
        ]

        if missing_destination_dimensions:
            source_reshape = [
                source[sidx].size for sidx in range(len(source))
            ] + ([1] * len(missing_destination_dimensions))
            # Add missing maps from the trailing 1-length dimensions that result from the reshape
            for didx in sorted([idx for idx in missing_destination_dimensions.values()]):
                source_transpose.append(didx)
        else:
            source_reshape = None

        source_from_destination: typing.List[typing.Optional[int]] = [None] * len(destination)
        for sidx in range(len(source)):
            didx = source_transpose[sidx]
            source_from_destination[didx] = sidx

        source_selection: typing.List[slice] = [
            slice(source[sidx].size) for sidx in range(len(source))
        ]
        source_apply: typing.List[typing.Optional[slice]] = [None] * len(destination)
        destination_apply: typing.List[typing.Optional[typing.Union[slice, np.ndarray]]] = [None] * len(destination)
        for didx in range(len(destination)):
            destination_dimension = destination[didx]
            sidx = source_from_destination[didx]
            if sidx is not None:
                source_dimension = source[sidx]
            else:
                source_dimension = None

            if destination_dimension.name == 'wavelength' and source_dimension:
                wavelength_source = contents.group().variables.get('wavelength')
                if wavelength_source is not None:
                    assign_mapping, dsize = _wavelength_mapping(wavelength_source, len(destination_dimension))
                    if sidx is not None:
                        source_selection[sidx] = slice(dsize)
                    else:
                        assert dsize == 1
                    source_apply[didx] = slice(dsize)
                    destination_apply[didx] = assign_mapping
                    continue
            if destination_dimension.name == 'cut_size':
                cut_size_destination = self.variable.group().variables.get('cut_size')
                cut_size_source = contents.group().variables.get('cut_size')
                if cut_size_destination is not None and cut_size_source is not None:
                    assign_mapping, dsize = _cut_size_mapping(cut_size_source, cut_size_destination)
                    if sidx is not None:
                        source_selection[sidx] = slice(dsize)
                    else:
                        assert dsize == 1
                    source_apply[didx] = slice(dsize)
                    destination_apply[didx] = assign_mapping
                    continue

            dsize = source_dimension.size if source_dimension else 1
            assert dsize <= destination_dimension.size

            source_apply[didx] = slice(dsize)
            destination_apply[didx] = slice(dsize)

        for i in range(len(source_transpose)):
            if source_transpose[i] != i:
                break
        else:
            source_transpose = []

        return _DataMapping(
            tuple([s.size for s in source]),
            tuple(source_selection),
            tuple(source_reshape) if source_reshape else None,
            tuple(source_transpose) if source_transpose else None,
            tuple(source_apply),
            tuple(destination_apply)
        )

    def convert_values(self, source: netCDF4.Variable, data: np.ndarray) -> typing.Optional[np.ndarray]:
        if self.enum_type:
            if isinstance(source.datatype, netCDF4.EnumType):
                result = MergeEnum.map_variables(source, self.variable, data)
                if result is not None:
                    return result

        try:
            return data.astype(self.dtype, copy=False)
        except ValueError:
            _LOGGER.debug("[%s] cast failed for %s->%s", self.name, data.dtype, self.dtype, exc_info=True)
            return None

    def map_data(self, contents: netCDF4.Variable, time_mapping: _TimeMapping) -> None:
        data_mapping = self.data_mapping(
            contents,
            [_find_dimension(contents.group(), name) for name in contents.dimensions[1:]],
            [_find_dimension(self.variable.group(), name) for name in self.variable.dimensions[1:]],
        )

        # Python compat: star expressions in subscriptions are not supported on older versions, so we just
        # manually construct the indexing tuple
        source_data = contents[(
            slice(time_mapping.source_start, time_mapping.source_end),
            *data_mapping.source_selection
        )]
        source_data = self.convert_values(contents, source_data)
        if source_data is None:
            return

        if data_mapping.source_reshape is not None:
            source_data = np.reshape(source_data, (time_mapping.count, *data_mapping.source_reshape))
        if data_mapping.source_transpose is not None:
            source_data = np.transpose(source_data, (0, *[i+1 for i in data_mapping.source_transpose]))

        for destination, source in data_mapping.netcdf_assignments(source_data.shape[1:]):
            self.variable[(
                slice(time_mapping.destination_start, time_mapping.destination_end),
                *destination
            )] = source_data[(
                slice(None),
                *source
            )]

    def apply_(self, start: typing.Optional[int], end: typing.Optional[int],
                            contents: netCDF4.Variable) -> None:
        pass

    def apply_time(self, start: typing.Optional[int], end: typing.Optional[int],
                   contents: netCDF4.Variable) -> typing.Optional[_TimeMapping]:
        return None

    def apply_data(self, start: int, end: int, contents: netCDF4.Variable, time_mapping: _TimeMapping) -> None:
        pass

    def finish(self) -> None:
        pass


class _ConstantVariable(_Variable):
    def apply_constant(self, contents: netCDF4.Variable) -> None:
        source_dimensions = contents.dimensions
        try:
            source_values = np.asarray(contents)
        except ValueError:
            # Handle automatic scalar conversion (e.x. str)
            source_values = np.array([contents[:]])
        if source_dimensions and source_dimensions[0] == 'time':
            source_dimensions = source_dimensions[1:]
            source_values = source_values[0, ...]

        data_mapping = self.data_mapping(
            contents,
            [_find_dimension(contents.group(), name) for name in source_dimensions],
            [_find_dimension(self.variable.group(), name) for name in self.variable.dimensions],
        )

        source_data = source_values[data_mapping.source_selection]
        source_data = self.convert_values(contents, source_data)
        if source_data is None:
            return

        if data_mapping.source_reshape is not None:
            source_data = np.reshape(source_data, data_mapping.source_reshape)
        if data_mapping.source_transpose is not None:
            source_data = np.transpose(source_data, data_mapping.source_transpose)

        for destination, source in data_mapping.netcdf_assignments(source_data.shape):
            self.variable[destination] = source_data[source]


class _HistoryConstantVariable(_Variable):
    def __init__(self, name: str, record: "_Record"):
        super().__init__(name, record)
        self.prior_value: typing.Optional[np.ndarray] = None
        self.history: typing.Dict[int, str] = dict()

    def _handle_existing_history(self, start: typing.Optional[int], end: typing.Optional[int],
                                 contents: netCDF4.Variable, data_mapping: _DataMapping) -> None:
        insert_history = _parse_history(
            getattr(contents, 'change_history', None),
            start, end,
        )

        for hkey in insert_history.keys():
            hvalue = insert_history[hkey].split(',')
            try:
                hvalue = np.reshape(hvalue, data_mapping.source_shape)
            except ValueError:
                if data_mapping.source_shape:
                    _LOGGER.debug("History value cannot be reshaped to data contents", exc_info=True)
                continue

            if data_mapping.source_reshape is not None:
                hvalue = np.reshape(hvalue, data_mapping.source_reshape)
            if data_mapping.source_transpose is not None:
                hvalue = np.transpose(hvalue, data_mapping.source_transpose)

            if self.variable.shape:
                hvalue = np.full(self.variable.shape, "", dtype=str)
                hvalue[data_mapping.destination_apply] = hvalue[data_mapping.source_apply]
                insert_history[hkey] = ",".join(hvalue.tolist())
            else:
                hvalue = hvalue[data_mapping.source_apply]
                insert_history[hkey] = str(hvalue)

        self.history.update(insert_history)

    def _update_history(self, start: typing.Optional[int], converted_value: np.ndarray) -> None:
        def equal_to_prior() -> bool:
            if np.issubdtype(self.dtype, np.floating):
                if not np.allclose(converted_value, self.prior_value, equal_nan=True):
                    return False
            else:
                if np.any(converted_value != self.prior_value):
                    return False
            return True

        if self.prior_value is not None and not equal_to_prior() and start is not None:
            format_code = getattr(self.variable, 'C_format', None)

            def format_history_item(value) -> str:
                try:
                    if value.mask:
                        return ""
                except AttributeError:
                    pass

                try:
                    value = float(value)
                except ValueError:
                    return str(value)

                if not isfinite(float(value)):
                    return ""
                if format_code:
                    try:
                        return format_code % value
                    except TypeError:
                        _LOGGER.debug("History format failed", exc_info=True)
                return str(value)

            format_list = self.prior_value.flatten().tolist()
            if not isinstance(format_list, list):
                self.history[start] = format_history_item(format_list)
            else:
                self.history[start] = ",".join([format_history_item(v) for v in format_list])

        self.prior_value = converted_value

    def apply_time(self, start: typing.Optional[int], end: typing.Optional[int],
                   contents: netCDF4.Variable) -> typing.Optional[_TimeMapping]:
        source_dimensions = contents.dimensions
        try:
            source_values = np.asarray(contents)
        except ValueError:
            # Handle automatic scalar conversion (e.x. str)
            source_values = np.array([contents[:]])
        if source_dimensions and source_dimensions[0] == 'time':
            source_dimensions = source_dimensions[1:]
            source_values = source_values[0, ...]

        data_mapping = self.data_mapping(
            contents,
            [_find_dimension(contents.group(), name) for name in source_dimensions],
            [_find_dimension(self.variable.group(), name) for name in self.variable.dimensions],
        )

        self._handle_existing_history(start, end, contents, data_mapping)

        source_data = source_values[data_mapping.source_selection]
        source_data = self.convert_values(contents, source_data)
        if source_data is None:
            return None

        if data_mapping.source_reshape is not None:
            source_data = np.reshape(source_data, data_mapping.source_reshape)
        if data_mapping.source_transpose is not None:
            source_data = np.transpose(source_data, data_mapping.source_transpose)

        for destination, source in data_mapping.netcdf_assignments(source_data.shape):
            self.variable[destination] = source_data[source]

        try:
            fill_value = self.variable._Fill_Value
        except AttributeError:
            if np.issubdtype(self.dtype, np.floating):
                fill_value = nan
            elif self.dtype == str:
                fill_value = ""
            else:
                fill_value = 0
        if self.variable.shape:
            converted_value = np.full(self.variable.shape, fill_value, dtype=self.dtype)
            # Don't need special handling because the destination is a proper Numpy array and the source selections
            # are always slices, so they are compatible with NetCDF indexing
            converted_value[data_mapping.destination_apply] = source_data[data_mapping.source_apply]
        else:
            converted_value = np.array(source_data[data_mapping.source_apply], dtype=self.dtype)

        self._update_history(start, converted_value)

        return None

    def finish(self) -> None:
        if not self.history:
            try:
                self.variable.delncattr('change_history')
            except (AttributeError, RuntimeError):
                pass
            return

        self.variable.setncattr('change_history', "\n".join([
            f"{format_iso8601_time(end / 1000.0)},{self.history[end]}"
            for end in sorted(self.history.keys())
        ]))


class _TimeVariable(_Variable):
    def __init__(self, record: "_Record"):
        super().__init__('time', record)
        self.is_state: bool = False
        self.next_time_index: int = 0
        self.last_time_value: typing.Optional[int] = None

    def incorporate_structure(self, contents: netCDF4.Variable, is_state: typing.Optional[bool]) -> None:
        super().incorporate_structure(contents, is_state)
        if is_state:
            self.is_state = is_state

    @property
    def time_dependent(self) -> bool:
        return True

    @property
    def is_time_dimension(self) -> bool:
        return True

    @property
    def bind_dimensions(self) -> typing.List[str]:
        return ['time'] + super().bind_dimensions

    def apply_time(self, start: typing.Optional[int], end: typing.Optional[int],
                   contents: netCDF4.Variable) -> typing.Optional[_TimeMapping]:
        if start is None or end is None:
            return None
        if len(contents.dimensions) == 0 or contents.dimensions[0] != 'time':
            return None
        if contents.shape[0] == 0:
            return None
        source_start = np.searchsorted(contents[:], start)
        source_end = np.searchsorted(contents[:], end)
        if source_end < contents.shape[0] and contents[source_end] < end:
            source_end += 1
        if self.is_state:
            # State incorporates the time before the interval, since that value overlaps the interval
            if source_start >= contents.shape[0]:
                source_start = contents.shape[0] - 1
            elif source_start > 0 and contents[source_start] > start:
                source_start -= 1
        if source_start >= contents.shape[0]:
            return None
        if source_start == source_end:
            return None

        time_count = source_end - source_start
        destination_start = self.next_time_index
        if self.last_time_value and contents[source_start] <= self.last_time_value:
            destination_start -= 1
        destination_end = destination_start + time_count

        self.next_time_index = destination_end
        self.last_time_value = contents[source_end - 1]

        mapping = _TimeMapping(source_start, source_end, destination_start, destination_end)
        self.map_data(contents, mapping)
        return mapping


class _DataVariable(_Variable):
    @property
    def time_dependent(self) -> bool:
        return True

    @property
    def bind_dimensions(self) -> typing.List[str]:
        return ['time'] + super().bind_dimensions

    def apply_data(self, start: int, end: int, contents: netCDF4.Variable, mapping: _TimeMapping) -> None:
        if len(contents.dimensions) == 0 or contents.dimensions[0] != 'time':
            _LOGGER.warning("Data variable without time dimension")
            return
        self.map_data(contents, mapping)


class _FlagsVariable(_DataVariable):
    def __init__(self, record: "_Record"):
        super().__init__('system_flags', record)
        self.flags: typing.Dict[str, int] = dict()
        self.bits: typing.Dict[int, str] = dict()
        self.dtype = np.uint64

    def incorporate_structure(self, contents: netCDF4.Variable, is_state: typing.Optional[bool]) -> None:
        super().incorporate_structure(contents, is_state)
        merge_flags = parse_flags(contents)
        unassigned_flags = list()
        for bit, flag in merge_flags.items():
            if self.flags.get(flag) is not None:
                continue
            if self.bits.get(bit) is None:
                self.flags[flag] = bit
                self.bits[bit] = flag
                continue
            unassigned_flags.append(bit)
        unassigned_flags.sort()
        for bit in unassigned_flags:
            flag = merge_flags[bit]
            for b in range(64):
                bit = 1 << b
                if self.bits.get(bit) is None:
                    break
            else:
                _LOGGER.warning("No free bit on %s for flag %s, ignoring", contents.name, flag)
                continue
            self.flags[flag] = bit
            self.bits[bit] = flag

    def declare_structure(self, root: netCDF4.Dataset) -> None:
        self.variable = root.createVariable(self.name, self.dtype, tuple(self.bind_dimensions), fill_value=False)
        variable_flags(self.variable, self.bits)

    def convert_values(self, source: netCDF4.Variable, data: np.ndarray) -> typing.Optional[np.ndarray]:
        merge_bits = parse_flags(source)
        if not merge_bits:
            return np.full(data.shape, 0, dtype=self.dtype)

        try:
            convert_bits = data.astype(np.uint64, copy=False)
        except ValueError:
            _LOGGER.debug("[%s] flags cast failed for %s", self.name, data.dtype, exc_info=True)
            return None

        for bit, flag in merge_bits.items():
            if self.bits.get(bit) == flag:
                continue
            break
        else:
            return convert_bits

        result = np.full(data.shape, 0, dtype=self.dtype)

        for bit, flag in merge_bits.items():
            present = (convert_bits & bit) != 0
            np.bitwise_or(result, self.flags[flag], where=present, out=result)

        return result


class _CutSizeVariable(_Variable):
    def __init__(self, record: "_Record"):
        super().__init__('cut_size', record)
        self.dtype = np.float64
        self.time_variable: typing.Optional[bool] = None
        self.constant_values: typing.Set[float] = set()

    def incorporate_structure(self, contents: netCDF4.Variable, is_state: typing.Optional[bool]) -> None:
        dimensions = contents.dimensions

        def incorporate_constant(cut_size):
            try:
                if cut_size.mask:
                    cut_size = nan
                else:
                    cut_size = float(cut_size)
            except AttributeError:
                cut_size = float(cut_size)

            self.constant_values.add(cut_size)

        if not dimensions:
            incorporate_constant(contents[:])
        elif len(dimensions) != 1:
            raise ValueError("Invalid cut size dimensionality")
        else:
            if dimensions[0] == 'cut_size':
                if self.time_variable == True:
                    raise ValueError("Unable to incorporate cut size dimension data with time dependant")
                self.time_variable = False
                for v in contents[:]:
                    incorporate_constant(v)
            elif dimensions[0] == 'time':
                if self.time_variable == False:
                    raise ValueError("Unable to incorporate cut size dimension data with time dependant")
                self.time_variable = True
            else:
                raise ValueError("Invalid cut size dimensionality")

        super().incorporate_structure(contents, is_state)

    def complete_structure(self) -> None:
        if self.time_variable is None and len(self.constant_values) > 1:
            self.time_variable = True
        if self.time_variable:
            return
        if len(self.constant_values) <= 1:
            self.record.dimension_size.pop('cut_size', None)
        else:
            self.record.dimension_size['cut_size'] = len(self.constant_values)

    @property
    def time_dependent(self) -> bool:
        return self.time_variable

    @property
    def bind_dimensions(self) -> typing.List[str]:
        if self.time_variable:
            return ['time']
        if len(self.constant_values) <= 1:
            return []
        else:
            return ['cut_size']

    def declare_structure(self, root: netCDF4.Dataset) -> None:
        self.variable = root.createVariable(self.name, self.dtype, tuple(self.bind_dimensions), fill_value=nan)
        variable_cutsize(self.variable)

    def apply_constant(self, contents: netCDF4.Variable) -> None:
        if self.time_variable:
            return
        try:
            self.constant_values.remove(nan)
            self.variable[:] = [nan] + sorted(self.constant_values)
        except KeyError:
            self.variable[:] = sorted(self.constant_values)

    def apply_data(self, start: int, end: int, contents: netCDF4.Variable, time_mapping: _TimeMapping) -> None:
        if not self.time_variable:
            return

        dimensions = contents.dimensions
        if not dimensions:
            source_data = contents[:]
        else:
            source_data = contents[time_mapping.source_start:time_mapping.source_end]
        source_data = self.convert_values(contents, source_data)
        if source_data is None:
            return

        if source_data.shape and source_data.shape[0] > 1:
            self.variable[time_mapping.destination_start:time_mapping.destination_end] = source_data[:]
        else:
            self.variable[time_mapping.destination_start:time_mapping.destination_end] = float(source_data)


class _Record:
    def __init__(self, root: netCDF4.Dataset):
        self.root = root
        self.groups: typing.Dict[str, _Record] = dict()
        self.dimension_size: typing.Dict[str, int] = dict()
        self.variables: typing.Dict[str, _Variable] = dict()
        self.enums: typing.Dict[str, MergeEnum] = dict()
        self.history_attrs: typing.List[_HistoryAttribute] = [
            _HistoryAttribute('instrument'),
        ]
        self.all_tags: typing.Set[str] = set()

    @staticmethod
    def _has_parent_group(g: netCDF4.Dataset, name: str) -> bool:
        while g is not None:
            if g.name == name:
                return True
            g = g.parent
        return False

    def _create_variable(self, var: netCDF4.Variable) -> _Variable:
        if var.name == 'cut_size':
            return _CutSizeVariable(self)

        dims = var.dimensions
        if not dims or dims[0] != 'time':
            if var.name == 'wavelength' and (not dims or dims[0] == 'wavelength'):
                return _HistoryConstantVariable(var.name, self)
            if self._has_parent_group(var.group(), 'instrument') and getattr(var, 'coverage_content_type', None) == "referenceInformation":
                return _HistoryConstantVariable(var.name, self)
            return _ConstantVariable(var.name, self)
        if var.name == 'time':
            return _TimeVariable(self)
        if var.name == 'system_flags':
            return _FlagsVariable(self)
        return _DataVariable(var.name, self)

    def incorporate_structure(self, contents: netCDF4.Dataset, is_parent_state: typing.Optional[bool] = None) -> None:
        for attr in contents.ncattrs():
            if attr.startswith('_'):
                continue
            if attr in self.root.ncattrs():
                continue
            self.root.setncattr(attr, contents.getncattr(attr))

        self.all_tags.update(getattr(contents, 'forge_tags', "").split())

        for name, dim in contents.dimensions.items():
            if dim.isunlimited():
                if name != "time":
                    _LOGGER.warning(f"Unlimited non-time dimension '%s'", name)
                continue
            self.dimension_size[name] = max(self.dimension_size.get(name, 0), dim.size)

        for name, etype in contents.enumtypes.items():
            target = self.enums.get(name)
            if not target:
                target = MergeEnum(name)
                self.enums[name] = target
            target.incorporate_structure(etype)

        is_state = is_state_group(contents)
        if is_state is None:
            is_state = is_parent_state

        for name, var in contents.variables.items():
            target = self.variables.get(name)
            if not target:
                target = self._create_variable(var)
                self.variables[name] = target
            target.incorporate_structure(var, is_state=is_state)

        for name, root in contents.groups.items():
            target = self.groups.get(name)
            if not target:
                target = _Record(self.root.createGroup(name))
                self.groups[name] = target
            target.incorporate_structure(root, is_parent_state=is_state)

    def declare_structure(self) -> None:
        for var in self.variables.values():
            var.complete_structure()
        for name, dsize in self.dimension_size.items():
            self.root.createDimension(name, dsize)
        for var in self.variables.values():
            if not var.time_dependent:
                continue
            if not var.is_time_dimension:
                check_group = self.root.parent
                outer_time_dimension = False
                while check_group is not None:
                    if 'time' in check_group.dimensions:
                        outer_time_dimension = True
                        break
                    check_group = check_group.parent
                if outer_time_dimension:
                    continue
            self.root.createDimension('time', None)
            break

        for edata in self.enums.values():
            edata.declare_structure(self.root)

        for var in self.variables.values():
            var.declare_structure(self.root)
        for group in self.groups.values():
            group.declare_structure()

    def incorporate_variables(self, contents: netCDF4.Dataset) -> None:
        for name, var in contents.variables.items():
            self.variables[name].incorporate_variable(var)
        for name, root in contents.groups.items():
            self.groups[name].incorporate_variables(root)

    def apply_constants(self, contents: netCDF4.Dataset) -> None:
        for name, var in contents.variables.items():
            self.variables[name].apply_constant(var)
        for name, root in contents.groups.items():
            self.groups[name].apply_constants(root)

    def apply_empty_data(self, contents: netCDF4.Dataset) -> None:
        for name, var in contents.variables.items():
            self.variables[name].apply_time(None, None, var)
        for name, root in contents.groups.items():
            self.groups[name].apply_empty_data(root)

    def apply_data(self, start: int, end: int, contents: netCDF4.Dataset,
                   time_mapping: typing.Optional[_TimeMapping] = None) -> None:
        for attr in self.history_attrs:
            attr.apply_data(start, end, contents)

        for name, var in contents.variables.items():
            result = self.variables[name].apply_time(start, end, var)
            if result:
                time_mapping = result

        if time_mapping:
            for name, var in contents.variables.items():
                self.variables[name].apply_data(start, end, var, time_mapping)

        for name, root in contents.groups.items():
            self.groups[name].apply_data(start, end, root, time_mapping)

    def finish(self) -> None:
        for attr in self.history_attrs:
            attr.finish(self.root)
        if self.all_tags:
            self.root.setncattr('forge_tags', " ".join(sorted(self.all_tags)))
        else:
            try:
                self.root.delncattr('forge_tags')
            except (AttributeError, RuntimeError):
                pass

        for var in self.variables.values():
            var.finish()
        for group in self.groups.values():
            group.finish()


class MergeInstrument:
    class _Source:
        def __init__(self, contents: netCDF4.Dataset,
                     not_before_ms: typing.Optional[int] = None,
                     not_after_ms: typing.Optional[int] = None):
            self.root = contents
            self.not_before_ms: typing.Optional[int] = not_before_ms
            self.not_after_ms: typing.Optional[int] = not_after_ms

            self.visibility: typing.List[typing.Tuple[int, int]] = list()

            self.file_start_time: typing.Optional[int] = None
            time_coverage_start = getattr(self.root, 'time_coverage_start', None)
            if time_coverage_start is not None:
                self.file_start_time = int(floor(parse_iso8601_time(str(time_coverage_start)).timestamp() * 1000.0))

            self.file_end_time: typing.Optional[int] = None
            time_coverage_end = getattr(self.root, 'time_coverage_end', None)
            if time_coverage_end is not None:
                self.file_end_time = int(ceil(parse_iso8601_time(str(time_coverage_end)).timestamp() * 1000.0))
                
            if self.file_start_time and self.file_end_time:
                effective_start = self.file_start_time
                if self.not_before_ms is not None:
                    effective_start = max(effective_start, self.not_before_ms)
                effective_end = self.file_end_time
                if self.not_after_ms is not None:
                    effective_end = min(effective_end, self.not_after_ms)
                if effective_start < effective_end:
                    self.visibility.append((effective_start, effective_end))

    def __init__(self):
        self._layers: typing.List["MergeInstrument._Source"] = list()

    def overlay(self, contents: netCDF4.Dataset,
                not_before_ms: typing.Optional[int] = None,
                not_after_ms: typing.Optional[int] = None) -> None:
        source = self._Source(contents, not_before_ms, not_after_ms)

        if not source.visibility:
            if not self._layers:
                self._layers.append(source)
            elif not self._layers[-1].visibility:
                self._layers.pop()
                self._layers.append(source)
            return

        visibility_start = source.visibility[0][0]
        visibility_end = source.visibility[0][1]

        existing_index = 0
        while existing_index < len(self._layers):
            existing = self._layers[existing_index]
            subtract_tuple(existing.visibility, visibility_start, visibility_end)
            if not existing.visibility:
                del self._layers[existing_index]
                continue
            existing_index += 1

        self._layers.append(source)

    def append(self, contents: netCDF4.Dataset) -> None:
        source = self._Source(contents)

        if not source.visibility:
            if not self._layers:
                self._layers.append(source)
            elif not self._layers[-1].visibility:
                self._layers.pop()
                self._layers.append(source)
            return

        visibility_start = source.visibility[0][0]
        visibility_end = MAX_I64
        existing_index = len(self._layers) - 1
        while existing_index >= 0:
            existing = self._layers[existing_index]
            subtract_tuple(existing.visibility, visibility_start, visibility_end)
            if not existing.visibility:
                del self._layers[existing_index]
                existing_index -= 1
                continue

            if existing.visibility[-1][1] <= visibility_start:
                break
            existing_index -= 1

        self._layers.append(source)

    def execute(self, output: typing.Union[str, "Path"]) -> netCDF4.Dataset:
        output = netCDF4.Dataset(str(output), 'w', format='NETCDF4')

        record = _Record(output)
        for layer in reversed(self._layers):
            record.incorporate_structure(layer.root)
        record.declare_structure()
        for layer in reversed(self._layers):
            record.incorporate_variables(layer.root)

        visible_sources: typing.List[typing.Tuple[int, int, "MergeInstrument._Source"]] = list()
        for layer in self._layers:
            for vis in layer.visibility:
                visible_sources.append((vis[0], vis[1], layer))
        visible_sources.sort(key=lambda x: x[0])

        for layer in self._layers:
            record.apply_constants(layer.root)
        if visible_sources:
            for start, end, source in visible_sources:
                record.apply_data(start, end, source.root)
        else:
            for layer in self._layers:
                record.apply_empty_data(layer.root)

        record.finish()

        if visible_sources and visible_sources[0][0]:
            output.time_coverage_start = format_iso8601_time(visible_sources[0][0] / 1000.0)
        if visible_sources and visible_sources[-1][1]:
            output.time_coverage_end = format_iso8601_time(visible_sources[-1][1] / 1000.0)

        return output
