import typing
import asyncio
import logging
import datetime
import netCDF4
import re
import functools
import numpy as np
from pathlib import Path
from math import nan, isfinite
from abc import ABC, abstractmethod
from ebas.io.file import nasa_ames
from ebas.io.ebasmetadata import DatasetCharacteristicList
from ebas.domain.basic_domain_logic.time_period import estimate_period_code, estimate_sample_duration_code, estimate_resolution_code
from nilutility.datatypes import DataObject
from forge.timeparse import parse_iso8601_time
from forge.product.selection import InstrumentSelection, VariableSelection
from forge.processing.station.lookup import station_data
from forge.archive.client.connection import Connection, LockBackoff, LockDenied
from forge.archive.client import index_lock_key, data_lock_key
from forge.data.dimensions import find_dimension_values
from forge.data.flags import parse_flags
from forge.data.state import is_state_group
from forge.data.merge.timealign import peer_output_time, incoming_before

_LOGGER = logging.getLogger(__name__)


async def _default_connection() -> Connection:
    return await Connection.default_connection('EBAS file generation')


class EBASFile(ABC):
    def __init__(self, station: str, start_epoch_ms: int, end_epoch_ms: int):
        self.station = station.lower()
        self.start_epoch_ms = start_epoch_ms
        self.end_epoch_ms = end_epoch_ms
        self.get_archive_connection: typing.Callable[[], typing.Awaitable[Connection]] = _default_connection

    class Variable:
        _FORMAT_CODE = re.compile(r'%[- #0+]*\d*(?:\.(\d+))?([diouxXeEfFgG])')

        def __init__(self, file: "EBASFile", metadata: typing.Optional[DataObject] = None, **kwargs):
            self.file = file
            self.digits: typing.Optional[int] = kwargs.pop("digits", None)
            self._variable_format: typing.Optional[str] = None

            if metadata is None:
                self.metadata = DataObject()
            else:
                self.metadata = metadata
            for attr, value in kwargs.items():
                setattr(self.metadata, attr, value)

            self._values: typing.List[typing.Tuple[np.ndarray, np.ndarray]] = list()

        def add_characteristic(self, *args) -> None:
            if getattr(self.metadata, 'characteristics', None) is None:
                self.metadata.characteristics = DatasetCharacteristicList()
            self.metadata.characteristics.add_parse(*args)

        def integrate_variable(
                self,
                var: netCDF4.Variable,
                selector: typing.Optional[typing.Dict[str, typing.Union[slice, int, np.ndarray]]] = None,
                converter: typing.Callable[[np.ndarray], np.ndarray] = None,
                allow_constant: bool = False,
                extra_vars: typing.List[typing.Union[netCDF4.Variable, np.ndarray]] = None,
        ) -> None:
            def broadcast_extra_var(evar: netCDF4.Variable) -> np.ndarray:
                # Broadcast to extra dimension on the end
                assert len(evar.shape) <= len(var.shape)
                evar_values = evar[:].data
                evar_values = np.reshape(evar_values, [*evar_values.shape] + ([1] * (len(var.shape) - len(evar.shape))))
                return np.broadcast_to(evar_values, var.shape)

            check = getattr(var, 'C_format', None)
            if check is not None:
                self._variable_format = str(check)

            if len(var.dimensions) == 0 or var.dimensions[0] != 'time':
                if not allow_constant:
                    _LOGGER.warning("Variable %s does not contain a time dimension", var.name)
                    return

                root = var.group()
                while True:
                    n = root.parent
                    if n is None:
                        break
                    root = n
                try:
                    start_time = parse_iso8601_time(str(root.time_coverage_start)).timestamp()
                except AttributeError:
                    start_time = self.file.start_epoch_ms

                time_values = np.array([start_time], dtype=np.int64)
                var_values = np.reshape(var[:].data, [1, *var.shape])
                var_dimensions = ['time', *var.dimensions]
                if extra_vars:
                    var_values = np.reshape(var_values, [*var_values.shape, 1])
                    var_dimensions.append('')
                    for evar in extra_vars:
                        evar_values = evar
                        if isinstance(evar_values, netCDF4.Variable):
                            evar_values = broadcast_extra_var(evar_values)
                        assert evar_values.shape == var_values.shape[1:-1]
                        evar_values = np.reshape(evar_values, [1, *var_values.shape[1:-1], 1])
                        var_values = np.vstack((var_values.T, evar_values.T)).T
            else:
                _, time_values = find_dimension_values(var.group(), 'time')
                time_values = time_values[:].data
                var_values = var[:].data
                var_dimensions = list(var.dimensions)
                if extra_vars:
                    var_values = np.reshape(var_values, [*var_values.shape, 1])
                    var_dimensions.append('')
                    for evar in extra_vars:
                        evar_values = evar
                        if isinstance(evar_values, netCDF4.Variable):
                            evar_values = broadcast_extra_var(evar_values)
                        assert evar_values.shape == var_values.shape[:-1]
                        evar_values = np.reshape(evar_values, [*var_values.shape[:-1], 1])
                        var_values = np.vstack((var_values.T, evar_values.T)).T
            assert len(time_values.shape) == 1
            if time_values.shape[0] == 0:
                return

            if selector:
                apply_selector = [slice(None)] * len(var_dimensions)
                time_selector = None
                for dim_name, dim_select in selector.items():
                    try:
                        dim_idx = var_dimensions.index(dim_name)
                    except ValueError:
                        _LOGGER.warning("Variable %s does not contain dimension", var.name, dim_name)
                        continue
                    apply_selector[dim_idx] = dim_select
                    if dim_name == 'time':
                        time_selector = dim_select
                var_values = var_values[tuple(apply_selector)]
                if time_selector is not None:
                    time_values = time_values[time_selector]
                    if time_values.shape[0] == 0:
                        return

            if converter:
                var_values = converter(var_values)

            value_total_size = 1
            for dim_size in var_values.shape:
                value_total_size *= dim_size
            if value_total_size == 0:
                return
            elif value_total_size == 1:
                var_values = np.full((time_values.shape[0],), var_values)
            elif value_total_size != time_values.shape[0]:
                raise ValueError(f"Final value shape for variable {var.name} is not compatible")
            else:
                var_values = np.reshape(var_values, (time_values.shape[0],))

            self._values.append((time_values, var_values))

        @property
        def has_any_valid(self) -> bool:
            for _, values in self._values:
                if np.issubdtype(values.dtype, np.floating):
                    if np.any(np.isfinite(values)):
                        return True
                else:
                    return True
            return False

        @property
        def all_time_data(self) -> typing.Iterator[np.ndarray]:
            for times, _ in self._values:
                yield times

        def get_values(self, times: np.ndarray, dtype=np.float64) -> np.ndarray:
            input_times = np.concatenate([v[0].astype(np.int64, casting='unsafe', copy=False) for v in self._values])
            input_values = np.concatenate([v[1].astype(dtype, casting='unsafe', copy=False) for v in self._values])
            assert input_times.shape == input_values.shape
            indices = incoming_before(times, input_times, sort_incoming=True)
            return input_values[indices]

        def declare_output(self, nas: nasa_ames.EbasNasaAmes,
                           values: np.ndarray, flags: typing.List[typing.List[int]]) -> None:
            round_digits: typing.Optional[int] = self.digits
            if round_digits is None and self._variable_format:
                check = self._FORMAT_CODE.search(self._variable_format)
                if check:
                    if not check.group(1):
                        round_digits = 0
                    else:
                        round_digits = int(check.group(1))

            if round_digits is not None:
                converted_values = [
                    (round(float(v), round_digits) if isfinite(v) else None)
                    for v in values
                ]
            else:
                converted_values = [
                    (float(v) if isfinite(v) else None)
                    for v in values
                ]

            nas.variables.append(DataObject(values_=converted_values, flags=flags, metadata=self.metadata))

    def variable(self, metadata: typing.Optional[DataObject] = None, **kwargs) -> "EBASFile.Variable":
        return self.Variable(self, metadata, **kwargs)

    class Flags:
        FLAG_BIT_ABNORMAL_DATA = 1 << 0
        _EMPTY_FLAGS: typing.List[int] = []
        _MISSING_FLAGS: typing.List[int] = [999]

        def __init__(self, file: "EBASFile"):
            self.file = file
            self._contents = file.Variable(file)

        def to_bits(self, flags: typing.Dict[int, str]) -> typing.Dict[int, int]:
            result: typing.Dict[int, int] = dict()
            for data_bit, flag_name in flags.items():
                if flag_name.startswith('abnormal_data_'):
                    result[data_bit] = self.FLAG_BIT_ABNORMAL_DATA
            return result

        def to_ebas_flags(self, bits: int) -> typing.List[int]:
            result: typing.List[int] = []
            if bits & self.FLAG_BIT_ABNORMAL_DATA:
                result.append(110)
            if not result:
                return self._EMPTY_FLAGS
            return result

        def integrate_file(
                self,
                root: netCDF4.Dataset,
                selector: typing.Optional[typing.Union[typing.Dict[str, typing.Union[slice, int, np.ndarray]], typing.Callable[[netCDF4.Variable], typing.Dict[str, typing.Union[slice, int, np.ndarray]]]]] = None,
        ) -> None:
            def process_system_flags(var: netCDF4.Variable):
                var_selector = None
                if selector is not None:
                    if callable(selector):
                        var_selector = selector(var)
                    else:
                        var_selector = selector

                flags_mapping = self.to_bits(parse_flags(var))
                if not flags_mapping:
                    return

                def converter(bits: np.ndarray) -> np.ndarray:
                    result = np.full((bits.shape[0], ), 0, dtype=np.uint64)
                    for check_bit, set_bit in flags_mapping.items():
                        hit = np.bitwise_and(bits, check_bit) != 0
                        set_at = np.any(hit, axis=tuple(range(1, len(hit.shape))))
                        result[set_at] = np.bitwise_or(result[set_at], set_bit)
                    return result

                self._contents.integrate_variable(var, var_selector, converter)

            def walk_group(g: netCDF4.Dataset):
                var = g.variables.get('system_flags')
                if var is not None:
                    process_system_flags(var)

                for name, sub in g.groups.items():
                    if name == 'statistics':
                        continue
                    walk_group(sub)

            walk_group(root)

        def get_flags(self, times: np.ndarray, valid_at_time: np.ndarray) -> typing.List[typing.List[int]]:
            if not self._contents.has_any_valid:
                return [
                    (self._EMPTY_FLAGS if v else self._MISSING_FLAGS) for v in valid_at_time
                ]

            flag_bits = self._contents.get_values(times, dtype=np.uint64)
            flag_bits[np.invert(valid_at_time)] = 0xFFFF_FFFF_FFFF_FFFF
            return [
                (self.to_ebas_flags(int(bits)) if int(bits) != 0xFFFF_FFFF_FFFF_FFFF else self._MISSING_FLAGS) for bits in flag_bits
            ]

    def flags(self) -> "EBASFile.Flags":
        return self.Flags(self)

    class MetadataTracker:
        def __init__(self, file: "EBASFile", path: str = 'instrument'):
            self.file = file
            self._path = path
            self.fields: typing.Dict[str, typing.Any] = dict()
            self.attributes: typing.Dict[str, typing.Any] = dict()

        @staticmethod
        def _simplify_value(value: typing.Union[netCDF4.Variable, np.ndarray]) -> typing.Any:
            if len(value.shape) == 0:
                value = value[0]
                if isinstance(value, str):
                    return str(value)
                if isinstance(value, int):
                    return int(value)
                if isinstance(value, float):
                    return float(value)
                if isinstance(value, np.ma.masked_array):
                    value = value.data
                if isinstance(value, np.ndarray):
                    if np.issubdtype(value.dtype, np.floating):
                        value = float(value)
                    elif np.issubdtype(value.dtype, np.integer):
                        value = int(value)
                    elif isinstance(value.dtype, str):
                        value = str(value)
                return value

            if isinstance(value, np.ma.masked_array):
                value = value.data
            return value.tolist()

        def integrate_file(self, root: netCDF4.Dataset) -> None:
            source = root
            for element in self._path.split('/'):
                if not element:
                    continue
                source = source.groups.get(element)
                if source is None:
                    return
            for attr_name in source.ncattrs():
                self.attributes[attr_name] = source.getncattr(attr_name)
            for field_name, var in source.variables.items():
                if len(var.dimensions) >= 1 and var.dimensions[0] == 'time':
                    self.fields[field_name] = self._simplify_value(var[0, ...])
                else:
                    self.fields[field_name] = self._simplify_value(var)

        def set_serial_number(self, nas: nasa_ames.EbasNasaAmes) -> None:
            if getattr(nas.metadata, 'instr_serialno', None) is not None:
                return
            serial_number = self.fields.get('serial_number')
            if not serial_number:
                return
            nas.metadata.instr_serialno = str(serial_number)

    def metadata_tracker(self, path: str = 'instrument') -> "EBASFile.MetadataTracker":
        return self.MetadataTracker(self, path)

    @staticmethod
    def quantile_converter(
            var: netCDF4.Variable,
            q: float
    ) -> typing.Callable[[np.ndarray], np.ndarray]:
        if len(var.dimensions) == 0 or var.dimensions[-1] != 'quantile':
            return lambda x: np.full(x.shape, nan, dtype=np.float64)
        try:
            _, quantiles = find_dimension_values(var.group(), 'quantile')
            if len(quantiles.shape) != 1 or quantiles.shape[0] == 0:
                raise KeyError
        except KeyError:
            return lambda x: np.full(x.shape, nan, dtype=np.float64)
        quantiles = quantiles[:].data

        idx_lower = int(np.searchsorted(quantiles, q, side='left'))
        if idx_lower >= len(quantiles):
            return lambda x: np.full(x.shape, nan, dtype=np.float64)
        if abs(float(quantiles[idx_lower]) - q) < 1e-6:
            return lambda x: x[..., idx_lower]
        if idx_lower == 0:
            return lambda x: np.full(x.shape, nan, dtype=np.float64)
        idx_lower -= 1

        idx_upper = int(np.searchsorted(quantiles, q, side='right'))
        if idx_upper == len(quantiles):
            return lambda x: x[..., idx_lower]
        if idx_lower == idx_upper:
            return lambda x: np.full(x.shape, nan, dtype=np.float64)

        q_lower = float(quantiles[idx_lower])
        q_upper = float(quantiles[idx_upper])
        if not isfinite(q_lower) or not isfinite(q_upper) or q_lower == q_upper:
            return lambda x: np.full(x.shape, nan, dtype=np.float64)

        def converter(x: np.ndarray) -> np.ndarray:
            v_lower = x[..., idx_lower]
            v_upper = x[..., idx_upper]
            return v_lower + (v_upper - v_lower) * (q - q_lower) / (q_upper - q_lower)

        return converter

    @staticmethod
    def limit_converter(
            limits: typing.Tuple[typing.Optional[float], typing.Optional[float]],
            chain: typing.Optional[typing.Callable[[np.ndarray], np.ndarray]] = None,
    ) -> typing.Callable[[np.ndarray], np.ndarray]:
        lower = limits[0]
        upper = limits[1]

        if lower is None and upper is None:
            if chain is not None:
                return chain
            return lambda x: x

        def converter(x: np.ndarray) -> np.ndarray:
            if chain is not None:
                x = chain(x)
            else:
                x = np.array(x)
            if lower is not None:
                if np.issubdtype(x.dtype, np.floating):
                    x[x <= lower] = nan
                else:
                    x[x < lower] = lower
            if upper is not None:
                if np.issubdtype(x.dtype, np.floating):
                    x[x >= upper] = nan
                else:
                    x[x > upper] = upper
            return x

        return converter

    @staticmethod
    def from_type_code(type_code: str) -> typing.Type["EBASFile"]:
        from importlib import import_module
        try:
            r = import_module('.' + type_code, 'forge.product.ebas.file')
            return r.File
        except (ModuleNotFoundError, AttributeError):
            raise FileNotFoundError(f"invalid EBAS output code {type_code}")

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        raise NotImplementedError

    @property
    def lab_code(self) -> str:
        return station_data(self.station, 'ebas', 'lab_code')(self.station, self.tags)

    @property
    def instrument_manufacturer(self) -> str:
        raise NotImplementedError

    @property
    def instrument_model(self) -> str:
        raise NotImplementedError

    @property
    def instrument_serial_number(self) -> typing.Optional[str]:
        return None

    @property
    def instrument_name(self) -> str:
        return f'{self.instrument_manufacturer}_{self.instrument_model}_{self.station.upper()}'

    @property
    def instrument_type(self) -> str:
        raise NotImplementedError

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        result = {
            'lab_code': self.lab_code,
            'instr_name': self.instrument_name,
            'instr_manufacturer': self.instrument_manufacturer,
            'instr_model': self.instrument_model,
            'instr_type': self.instrument_type,
            'instr_serialno': self.instrument_serial_number,
            'license': station_data(self.station, 'dataset', 'license')(self.station, self.tags),
            'acknowledgements': 'Request acknowledgement details from data originator',
        }
        # EBAS DOI is per-file, so but we're setting the DOI for the whole dataset, so set the 'Contains data from DOI'
        # header instead.
        # Also, per file DOI handling is broken (ebas-io tries to import something that doesn't exist) and would
        # make the filenames "wrong" even if it worked.
        doi = station_data(self.station, 'dataset', 'doi')(self.station, self.tags)
        if doi:
            result['doi_list'] = [doi]
        return result

    @property
    def level0_metadata(self) -> typing.Dict[str, str]:
        return {
            'datalevel': '0',
            'duration': '1mn',
            'resolution': '1mn',
            'type': 'TI',
        }

    @property
    def level1_metadata(self) -> typing.Dict[str, str]:
        return {
            'datalevel': '1',
            'duration': '1mn',
            'resolution': '1mn',
            'type': 'TI',
        }

    @property
    def level2_metadata(self) -> typing.Dict[str, str]:
        return {
            'datalevel': '2',
            'duration': '1h',
            'resolution': '1h',
            'type': 'TU',
        }

    @classmethod
    def with_file_metadata(cls, metadata: typing.Dict[str, str]) -> typing.Type["EBASFile"]:
        class WithMetdata(cls):
            @property
            def file_metadata(self) -> typing.Dict[str, str]:
                r = super().file_metadata
                r.update(metadata)
                return r
        return WithMetdata

    async def fetch_instrument_files(
            self,
            selections: typing.Iterable[InstrumentSelection],
            archive: str,
            destination_directory: Path,
    ) -> None:
        async with await self.get_archive_connection() as connection:
            backoff = LockBackoff()
            while True:
                try:
                    async with connection.transaction():
                        await connection.lock_read(index_lock_key(self.station, archive),
                                                   self.start_epoch_ms, self.end_epoch_ms)
                        await connection.lock_read(data_lock_key(self.station, archive),
                                                   self.start_epoch_ms, self.end_epoch_ms)
                        for sel in selections:
                            await sel.fetch_files(connection, self.station, archive,
                                                  self.start_epoch_ms, self.end_epoch_ms,
                                                  destination_directory)
                    break
                except LockDenied as ld:
                    _LOGGER.debug("Archive busy: %s", ld.status)
                    await backoff()
                    continue

    @staticmethod
    def select_variable(
            root: netCDF4.Dataset,
            *selections: typing.Union[typing.Dict, VariableSelection, str],
            statistics: typing.Optional[str] = None,
            allow_constant: bool = False,
    ) -> typing.Iterator[netCDF4.Variable]:
        yield from VariableSelection.find_matching_variables(
            root, *selections,
            statistics=statistics,
            allow_constant=allow_constant,
        )

    @staticmethod
    async def iter_data_files(data_directory: Path) -> typing.AsyncIterable[netCDF4.Dataset]:
        for file_path in data_directory.iterdir():
            root = netCDF4.Dataset(str(file_path), 'r')
            try:
                yield root
            finally:
                root.close()
            await asyncio.sleep(0)

    class MatrixData:
        def __init__(self, files: "EBASFile"):
            self.files = files
            self._nas: typing.Dict[str, nasa_ames.EbasNasaAmes] = dict()

        def __getitem__(self, matrix: str) -> nasa_ames.EbasNasaAmes:
            nas = self._nas.get(matrix)
            if nas is None:
                nas = self.files.begin_file()
                nas.metadata.matrix = matrix
                self._nas[matrix] = nas
            return nas

        def __iter__(self) -> typing.Iterator[nasa_ames.EbasNasaAmes]:
            yield from self._nas.values()

        class Selector:
            def __init__(self, cut_size: float):
                self.cut_size = cut_size

            def __call__(
                    self,
                    var: netCDF4.Variable,
                    require_cut_size_match: bool = True,
            ) -> typing.Dict[str, typing.Union[slice, int, np.ndarray]]:
                if 'cut_size' in var.dimensions:
                    _, cut_var =  find_dimension_values(var.group(), 'cut_size')
                    cut_values = cut_var[:].data
                    if isfinite(self.cut_size):
                        selected = cut_values == self.cut_size
                    else:
                        selected = np.invert(np.isfinite(cut_values))
                    return {'cut_size': selected}

                if 'cut_size' not in getattr(var, 'ancillary_variables', "").split():
                    if isfinite(self.cut_size) and require_cut_size_match:
                        return {'time': slice(0)}
                    return {}

                cut_var = var.group().variables.get('cut_size')
                if cut_var is None:
                    _LOGGER.warning(f"No sibling cut size variable for {var.name}")
                    return {}
                if len(cut_var.dimensions) != 1 or cut_var.dimensions[0] != 'time':
                    if isfinite(self.cut_size):
                        any_hit = np.any(cut_var[:].data == self.cut_size)
                    else:
                        any_hit = np.any(np.invert(np.isfinite(cut_var[:].data)))
                    if not any_hit and require_cut_size_match:
                        return {'time': slice(0)}
                    return {}

                if len(var.dimensions) == 0 or var.dimensions[0] != 'time':
                    return {'time': slice(0)}

                cut_values = cut_var[:].data
                if isfinite(self.cut_size):
                    selected = cut_values == self.cut_size
                else:
                    selected = np.invert(np.isfinite(cut_values))
                return {'time': selected}

        class Context:
            def __init__(self, ctor: typing.Callable, *args, **kwargs):
                self._contents: typing.Dict[nasa_ames.EbasNasaAmes, typing.Any] = dict()
                self._ctor = ctor
                self._ctor_args = args
                self._ctor_kwargs = kwargs

            def __getitem__(self, nas: nasa_ames.EbasNasaAmes):
                value = self._contents.get(nas)
                if value is None:
                    value = self._ctor(*self._ctor_args, **self._ctor_kwargs)
                    self._contents[nas] = value
                return value

            def __iter__(self):
                yield from self._contents.values()

            def keys(self):
                return self._contents.keys()

            def values(self):
                return self._contents.values()

            def items(self):
                return self._contents.items()

            def variables(self):
                return self.values()

        def context(self, ctor: typing.Callable, *args, **kwargs) -> "EBASFile.MatrixData.Context":
            return self.Context(ctor, *args, **kwargs)

        def variable(self, **kwargs) -> "EBASFile.MatrixData.Context":
            return self.context(self.files.variable, None, **kwargs)

        def flags(self) -> "EBASFile.MatrixData.Context":
            return self.context(self.files.flags)

        def metadata_tracker(self, path: str = 'instrument') -> "EBASFile.MatrixData.Context":
            return self.context(functools.partial(self.files.metadata_tracker, path=path))

        @staticmethod
        def _find_available_cut_sizes(root: netCDF4.Dataset) -> typing.List[float]:
            def _exclude_variable(variable: netCDF4.Variable) -> bool:
                if 'time' not in variable.dimensions:
                    # Only care about timeseries variables for finding available cut sizes
                    return True
                if variable.name == 'averaged_time':
                    return True
                if variable.name == 'averaged_count':
                    return True
                if variable.name == 'system_flags':
                    return True
                return False

            seen_cut_sizes: typing.Set[float] = set()
            seen_whole_air: bool = False

            def add_cut_sizes(cut_values: np.ndarray):
                valid_sizes = np.isfinite(cut_values)
                cut_values = cut_values[valid_sizes]
                seen_cut_sizes.update(cut_values)
                if np.any(np.invert(valid_sizes)):
                    nonlocal seen_whole_air
                    seen_whole_air = True

            def process_group(g: netCDF4.Dataset):
                for var in g.variables.values():
                    if _exclude_variable(var):
                        continue

                    if 'cut_size' in var.dimensions:
                        _, cut_var = find_dimension_values(var.group(), 'cut_size')
                        add_cut_sizes(cut_var[:].data)
                        continue

                    if 'cut_size' not in getattr(var, 'ancillary_variables', "").split():
                        nonlocal seen_whole_air
                        seen_whole_air = True
                        continue

                    cut_var = var.group().variables.get('cut_size')
                    if cut_var is None:
                        continue
                    add_cut_sizes(cut_var[:].data)

            def walk_group(g: netCDF4.Dataset):
                if is_state_group(g):
                    return
                if g.name == 'statistics':
                    return
                process_group(g)
                for sub in g.groups.values():
                    walk_group(sub)

            process_group(root)
            for name, sub in root.groups.items():
                if name == 'instrument':
                    continue
                walk_group(sub)

            cut_sizes = sorted(seen_cut_sizes)
            if seen_whole_air:
                cut_sizes.append(nan)
            return cut_sizes

        async def iter_data_files(self, data_directory: Path) -> typing.AsyncIterable[typing.Tuple[nasa_ames.EbasNasaAmes, "EBASFile.MatrixData.Selector", netCDF4.Dataset]]:
            async for root in self.files.iter_data_files(data_directory):
                cut_sizes = self._find_available_cut_sizes(root)
                _LOGGER.debug(f"Processing {len(cut_sizes)} candidate cut sizes in {Path(root.filepath()).name}")
                for cs in cut_sizes:
                    if cs == 1.0:
                        yield self["pm1"], self.Selector(cs), root
                    elif cs == 2.5:
                        yield self["pm25"], self.Selector(cs), root
                    elif cs == 10.0:
                        yield self["pm10"], self.Selector(cs), root
                    elif not isfinite(cs):
                        yield self["aerosol"], self.Selector(cs), root
                    else:
                        _LOGGER.debug(f"Skipping cut size {cs} for file {Path(root.filepath()).name} due to no EBAS matrix assignment")

    def begin_file(self) -> nasa_ames.EbasNasaAmes:
        nas = nasa_ames.EbasNasaAmes()
        station_data(self.station, 'ebas', 'set_metadata')(nas, self.station, self.tags)
        for key, value in self.file_metadata.items():
            if not value:
                continue
            setattr(nas.metadata, key, value)
        return nas

    def apply_times(
            self,
            nas: nasa_ames.EbasNasaAmes,
            start_time_epoch_ms: np.ndarray,
            end_time_epoch_ms: np.ndarray,
    ) -> None:
        nas.sample_times = [(
            datetime.datetime.fromtimestamp(
                int(start_time_epoch_ms[i]) / 1000.0,
                tz=datetime.timezone.utc,
            ),
            datetime.datetime.fromtimestamp(
                int(end_time_epoch_ms[i]) / 1000.0,
                tz=datetime.timezone.utc,
            ),
        ) for i in range(start_time_epoch_ms.shape[0])]
        # nas.metadata.period = estimate_period_code(nas.sample_times[0][0], nas.sample_times[-1][1])
        start_datetime = datetime.datetime.fromtimestamp(
            self.start_epoch_ms / 1000.0,
            tz=datetime.timezone.utc,
        )
        end_datetime = datetime.datetime.fromtimestamp(
            self.end_epoch_ms / 1000.0,
            tz=datetime.timezone.utc,
        )
        nas.metadata.period = estimate_period_code(start_datetime, end_datetime)
        nas.metadata.reference_date = datetime.datetime(start_datetime.year, 1, 1,
                                                        tzinfo=datetime.timezone.utc)
        if nas.metadata.duration is None:
            nas.metadata.duration = estimate_sample_duration_code(nas.sample_times)
        if nas.metadata.resolution is None:
            nas.metadata.resolution = estimate_resolution_code(nas.sample_times)

    @staticmethod
    def assemble_values(
            times: np.ndarray,
            variables: typing.List["EBASFile.Variable"],
            optional: typing.List["EBASFile.Variable"],
    ) -> typing.Tuple[np.ndarray, np.ndarray]:
        output_values = np.stack(tuple(
            [v.get_values(times) for v in variables] +
            [v.get_values(times) for v in optional]
        ), axis=1)
        valid_at_time = np.any(np.isfinite(output_values[:, 0:len(variables)]), axis=1)
        return output_values, valid_at_time

    def declare_variables(
            self,
            nas: nasa_ames.EbasNasaAmes,
            variables: typing.Iterable["EBASFile.Variable"],
            optional: typing.Iterable["EBASFile.Variable"] = None,
            flags: typing.Optional["EBASFile.Flags"] = None,
            fixed_start_epoch_ms: typing.Optional[np.ndarray] = None,
            fixed_end_epoch_ms: typing.Optional[np.ndarray] = None,
    ) -> bool:
        valid_variables: typing.List["EBASFile.Variable"] = list()
        for var in variables:
            if not var.has_any_valid:
                continue
            valid_variables.append(var)
        variables = valid_variables
        if not variables:
            return False

        valid_optional: typing.List["EBASFile.Variable"] = list()
        if optional is not None:
            for var in optional:
                if not var.has_any_valid:
                    continue
                valid_optional.append(var)
        optional = valid_optional

        end_time_epoch_ms = None
        if fixed_start_epoch_ms is not None:
            if fixed_start_epoch_ms.shape[0] == 0:
                return False
            start_time_epoch_ms = fixed_start_epoch_ms
            if fixed_end_epoch_ms is not None:
                end_time_epoch_ms = fixed_end_epoch_ms
            else:
                end_time_epoch_ms = np.concatenate((
                    fixed_start_epoch_ms[:1],
                    [fixed_start_epoch_ms[-1] + (fixed_start_epoch_ms[-1] - fixed_start_epoch_ms[-2])]
                ))
        else:
            start_time_epoch_ms = peer_output_time(*[t for var in variables for t in var.all_time_data])
            start_time_epoch_ms = start_time_epoch_ms[np.all((
                start_time_epoch_ms >= self.start_epoch_ms,
                start_time_epoch_ms < self.end_epoch_ms
            ), axis=0)]
            if start_time_epoch_ms.shape[0] == 0:
                return False
        if end_time_epoch_ms is None:
            if start_time_epoch_ms.shape[0] < 2:
                return False
            time_difference = start_time_epoch_ms[1:] - start_time_epoch_ms[:-1]
            time_step_values, time_step_count = np.unique(time_difference, return_counts=True)
            time_step = time_step_values[np.argmax(time_step_count)]
            if time_step == 0:
                return False
            end_time_epoch_ms = start_time_epoch_ms + time_step
            valid_times = end_time_epoch_ms <= self.end_epoch_ms
            start_time_epoch_ms = start_time_epoch_ms[valid_times]
            end_time_epoch_ms = end_time_epoch_ms[valid_times]
            if start_time_epoch_ms.shape[0] == 0:
                return False
            check_overlap_end = end_time_epoch_ms[:-1]
            check_overlap_next = end_time_epoch_ms[:-1]
            overlapping = check_overlap_end > check_overlap_next
            check_overlap_end[overlapping] = check_overlap_next[overlapping]

        file_values, valid_at_time = self.assemble_values(start_time_epoch_ms, variables, optional)
        if not np.any(valid_at_time):
            return False
        file_values[np.invert(valid_at_time)] = nan
        if flags:
            file_flags = flags.get_flags(start_time_epoch_ms, valid_at_time)
        else:
            empty_flags = []
            missing_flags = [999]
            file_flags = [
                (empty_flags if v else missing_flags) for v in valid_at_time
            ]

        self.apply_times(nas, start_time_epoch_ms, end_time_epoch_ms)
        file_value_index = 0
        for v in variables:
            v.declare_output(nas, file_values[:, file_value_index], file_flags)
            file_value_index += 1
        for v in optional:
            v.declare_output(nas, file_values[:, file_value_index], file_flags)
            file_value_index += 1

        return True

    async def assemble_file(
            self,
            nas: nasa_ames.EbasNasaAmes, output_directory: Path,
            variables: typing.Iterable["EBASFile.Variable"],
            optional: typing.Iterable["EBASFile.Variable"] = None,
            flags: typing.Optional["EBASFile.Flags"] = None,
            fixed_interval_ms: typing.Optional[int] = None,
    ) -> None:
        if fixed_interval_ms:
            fixed_start_epoch_ms = np.arange(self.start_epoch_ms, self.end_epoch_ms, fixed_interval_ms)
            fixed_end_epoch_ms = fixed_start_epoch_ms + fixed_interval_ms
        else:
            fixed_start_epoch_ms = None
            fixed_end_epoch_ms = None
        _LOGGER.debug(f"Processing matrix {getattr(nas.metadata, 'matrix', 'UNKNOWN')}")
        if not self.declare_variables(nas, variables, optional, flags, fixed_start_epoch_ms, fixed_end_epoch_ms):
            _LOGGER.debug(f"Skipping output of matrix {getattr(nas.metadata, 'matrix', 'UNKNOWN')}")
            return
        _LOGGER.debug(f"Writing file for matrix {getattr(nas.metadata, 'matrix', 'UNKNOWN')}")
        await self.write_file(nas, output_directory)

    @staticmethod
    async def write_file(nas: nasa_ames.EbasNasaAmes, output_directory: Path) -> None:
        def _write():
            nas.write(createfiles=True, destdir=str(output_directory))

        await asyncio.get_event_loop().run_in_executor(None, _write)

    @abstractmethod
    async def __call__(self, output_directory: Path) -> None:
        pass
