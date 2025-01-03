import typing
import asyncio
import logging
import time
import netCDF4
import numpy as np
from pathlib import Path
from math import floor, isfinite, nan
from abc import ABC, abstractmethod
from forge.const import MAX_I64
from forge.formattime import format_iso8601_time
from forge.timeparse import parse_iso8601_time
from forge.product.selection import InstrumentSelection, VariableSelection
from forge.archive.client.connection import Connection, LockBackoff, LockDenied
from forge.archive.client import index_lock_key, data_lock_key
from forge.data.history import parse_history
from forge.data.dimensions import find_dimension_values
from forge.data.merge.timeselect import selected_time_range

_LOGGER = logging.getLogger(__name__)


async def _default_connection() -> Connection:
    return await Connection.default_connection('NCEI file generation')


class NCEIFile(ABC):
    def __init__(self, station: str, start_epoch_ms: int, end_epoch_ms: int):
        self.station = station.lower()
        self.start_epoch_ms = start_epoch_ms
        self.end_epoch_ms = end_epoch_ms
        self.get_archive_connection: typing.Callable[[], typing.Awaitable[Connection]] = _default_connection

    @property
    def archive(self) -> str:
        return "avgh"

    @property
    def time_coverage_resolution(self) -> float:
        return 60 * 60

    @property
    def tags(self) -> typing.Set[str]:
        raise NotImplementedError

    @property
    def file_root_name(self) -> str:
        raise NotImplementedError

    def apply_file_metadata(self, root: netCDF4.Dataset, file_creation_time: float) -> None:
        from forge.data.structure.basic import set_basic
        from forge.data.structure.dataset import set_dataset
        from forge.data.structure.site import set_site
        from forge.data.structure.ebas import set_ebas
        from forge.data.structure.timeseries import date_created, time_coverage_start, time_coverage_end, time_coverage_resolution
        from forge.data.structure.history import append_history

        set_basic(root)
        set_dataset(root, self.station, tags=self.tags)
        set_site(root, self.station, tags=self.tags)
        set_ebas(root, self.station, tags=self.tags)

        date_created(root, file_creation_time)
        root.setncattr("id", f"{self.file_root_name}_{self.station.upper()}_s{format_iso8601_time(self.start_epoch_ms/1000)}_e{format_iso8601_time(self.end_epoch_ms / 1000)}_c{format_iso8601_time(file_creation_time)}")
        time_coverage_start(root, self.start_epoch_ms / 1000)
        time_coverage_end(root, self.end_epoch_ms / 1000)
        time_coverage_resolution(root, self.time_coverage_resolution)

        append_history(root, "forge.ncei", file_creation_time)

    @staticmethod
    def from_type_code(type_code: str) -> typing.Type["NCEIFile"]:
        from importlib import import_module
        try:
            r = import_module('.' + type_code, 'forge.product.ncei.file')
            return r.File
        except (ModuleNotFoundError, AttributeError):
            raise FileNotFoundError(f"invalid NCEI output code {type_code}")

    async def fetch_instrument_files(
            self,
            selections: typing.Dict[str, typing.Iterable[InstrumentSelection]],
            destination_directory: Path,
    ) -> None:
        async with await self.get_archive_connection() as connection:
            backoff = LockBackoff()
            while True:
                try:
                    async with connection.transaction():
                        await connection.lock_read(index_lock_key(self.station, self.archive),
                                                   self.start_epoch_ms, self.end_epoch_ms)
                        await connection.lock_read(data_lock_key(self.station, self.archive),
                                                   self.start_epoch_ms, self.end_epoch_ms)
                        for subdir, dir_selections in selections.items():
                            for sel in dir_selections:
                                await sel.fetch_files(connection, self.station, self.archive,
                                                      self.start_epoch_ms, self.end_epoch_ms,
                                                      destination_directory / subdir)
                    break
                except LockDenied as ld:
                    _LOGGER.debug("Archive busy: %s", ld.status)
                    await backoff()
                    continue

    @staticmethod
    async def iter_data_files(data_directory: Path) -> typing.AsyncIterable[netCDF4.Dataset]:
        for file_path in data_directory.iterdir():
            root = netCDF4.Dataset(str(file_path), 'r')
            try:
                yield root
            finally:
                root.close()
            await asyncio.sleep(0)

    class MergeInstrument:
        class _Source:
            def __init__(self, file_time: int, manufacturer: typing.Optional[str],
                         model: typing.Optional[str], serial_number: typing.Optional[str]):
                self.time = file_time
                self.manufacturer = manufacturer
                self.model = model
                self.serial_number = serial_number

            def merge_from(self, other: "NCEIFile.MergeInstrument._Source") -> None:
                if not self.manufacturer:
                    self.manufacturer = other.manufacturer
                if not self.model:
                    self.model = other.model
                if not self.serial_number:
                    self.serial_number = other.serial_number

            def __eq__(self, other):
                if not isinstance(other, NCEIFile.MergeInstrument._Source):
                    return NotImplemented
                return self.manufacturer == other.manufacturer and self.model == other.model and self.serial_number == other.serial_number

            def __ne__(self, other):
                return not self.__eq__(other)

        def __init__(self):
            self._history: typing.Optional[str] = None
            self._instrument_source: typing.List[NCEIFile.MergeInstrument._Source] = list()

        def _integrate_source(self, root: netCDF4.Dataset) -> None:
            meta = root.groups.get("instrument")
            if meta is None:
                return

            time_coverage_start = getattr(root, 'time_coverage_start', None)
            if time_coverage_start is None:
                return
            time_coverage_start = int(floor(parse_iso8601_time(str(time_coverage_start)).timestamp() * 1000.0))

            manufacturer = meta.variables.get("manufacturer")
            model = meta.variables.get("model")
            serial_number = meta.variables.get("serial_number")
            if manufacturer is None and model is None and serial_number is None:
                return

            changed_times: typing.Dict[int, typing.Tuple[str, str, str]] = dict()
            if manufacturer is not None:
                for t, v in parse_history(getattr(manufacturer, 'change_history', None)).items():
                    existing = changed_times.get(t, (None, None, None))
                    changed_times[t] = (v, existing[1], existing[2])
            if model is not None:
                for t, v in parse_history(getattr(model, 'change_history', None)).items():
                    existing = changed_times.get(t, (None, None, None))
                    changed_times[t] = (existing[0], v, existing[2])
            if serial_number is not None:
                for t, v in parse_history(getattr(serial_number, 'change_history', None)).items():
                    existing = changed_times.get(t, (None, None, None))
                    changed_times[t] = (existing[0], existing[1], v)

            latest_time = time_coverage_start
            for next_time in sorted(changed_times.keys()):
                add = changed_times[next_time]
                self._instrument_source.append(self._Source(latest_time, *add))
                latest_time = next_time

            self._instrument_source.append(self._Source(
                latest_time,
                str(manufacturer[0]) if manufacturer is not None else None,
                str(model[0]) if model is not None else None,
                str(serial_number[0]) if serial_number is not None else None,
            ))

        def integrate_file(self, root: netCDF4.Dataset) -> None:
            self._integrate_source(root)

            history = getattr(root, 'history', None)
            if history is not None:
                history = str(history)
                if history:
                    self._history = history

        def _write_instrument_source(self, group: netCDF4.Group) -> None:
            if not self._instrument_source:
                return
            self._instrument_source.sort(key=lambda x: x.time)

            # Fill missing forwards and backwards (assume no change when missing)
            for i in range(len(self._instrument_source)-1):
                self._instrument_source[i+1].merge_from(self._instrument_source[i])
            for i in reversed(range(len(self._instrument_source)-1)):
                self._instrument_source[i].merge_from(self._instrument_source[i+1])

            # Deduplicate
            for i in reversed(range(1, len(self._instrument_source))):
                if self._instrument_source[i] == self._instrument_source[i-1]:
                    del self._instrument_source[i]

            dest = group.createGroup("instrument")
            if len(self._instrument_source) == 1:
                manufacturer = dest.createVariable("manufacturer", str, ())
                manufacturer.long_name = "instrument manufacturer"
                manufacturer.coverage_content_type = "auxiliaryInformation"
                manufacturer[0] = self._instrument_source[0].manufacturer

                model = dest.createVariable("model", str, ())
                model.long_name = "instrument model"
                model.coverage_content_type = "auxiliaryInformation"
                model[0] = self._instrument_source[0].model

                serial_number = dest.createVariable("serial_number", str, ())
                serial_number.long_name = "instrument serial number"
                serial_number.coverage_content_type = "auxiliaryInformation"
                serial_number[0] = self._instrument_source[0].serial_number
            else:
                dest.createDimension("instrument_change_time", len(self._instrument_source))
                t = dest.createVariable("instrument_change_time", 'f8', ('instrument_change_time',))
                t.long_name = "start time of description"
                t.standard_name = "time"
                t.coverage_content_type = "coordinate"
                t.units = "seconds since 1970-01-01 00:00:00"
                t[:] = [s.time / 1000.0 for s in self._instrument_source]

                manufacturer = dest.createVariable("manufacturer", str, ('instrument_change_time',))
                manufacturer.long_name = "instrument manufacturer"
                manufacturer.coverage_content_type = "auxiliaryInformation"

                model = dest.createVariable("model", str, ('instrument_change_time',))
                model.long_name = "instrument model"
                model.coverage_content_type = "auxiliaryInformation"

                serial_number = dest.createVariable("serial_number", str, ('instrument_change_time',))
                serial_number.long_name = "instrument serial number"
                serial_number.coverage_content_type = "auxiliaryInformation"

                for i in range(len(self._instrument_source)):
                    s = self._instrument_source[i]
                    manufacturer[i] = s.manufacturer or ""
                    model[i] = s.model or ""
                    serial_number[i] = s.serial_number or ""

        def write(self, group: netCDF4.Group) -> None:
            self._write_instrument_source(group)

            if self._history:
                group.history = self._history

    class MergeVariable:
        def __init__(self):
            self.times = np.empty((0,), dtype=np.int64)
            self.values = np.empty((0,0,0), dtype=np.float64)
            self._wavelengths: typing.List[typing.Optional[float]] = list()
            self._cut_sizes: typing.List[typing.Optional[float]] = list()

        def integrate_variable(self, root: netCDF4.Dataset, var: netCDF4.Variable) -> None:
            if len(var.dimensions) == 0 or var.dimensions[0] != 'time':
                return
            _, time_var = find_dimension_values(var.group(), 'time')
            times = time_var[:].data
            if times.shape[0] == 0:
                return
            times = times.astype(np.int64, casting='unsafe', copy=False)

            time_coverage_start = getattr(root, 'time_coverage_start', None)
            if time_coverage_start is None:
                time_coverage_start = int(times[0])
            else:
                time_coverage_start = int(floor(parse_iso8601_time(str(time_coverage_start)).timestamp() * 1000.0))

            try:
                wavelength_idx = var.dimensions.index('wavelength')
            except ValueError:
                wavelength_idx = None
            try:
                cut_size_idx = var.dimensions.index('cut_size')
            except ValueError:
                cut_size_idx = None

            data_selector: typing.List[typing.Union[int, slice]] = [slice(0, 1)] * len(var.dimensions)
            data_selector[0] = slice(None)
            if wavelength_idx is not None:
                data_selector[wavelength_idx] = slice(None)
            if cut_size_idx is not None:
                data_selector[cut_size_idx] = slice(None)

            def finite_or_none(v) -> typing.Optional[float]:
                if v is None:
                    return None
                try:
                    v = float(v)
                except ValueError:
                    return None
                if not isfinite(v):
                    return None
                return v

            values = np.asarray(var[:].data)[tuple(data_selector)]
            values = values.astype(np.float64, casting='unsafe', copy=False)
            if wavelength_idx is None:
                values = values.reshape((*values.shape, 1))
                wavelength_idx = len(values.shape) - 1
                wavelength_values = {time_coverage_start: [None]}
            else:
                _, wavelength_var = find_dimension_values(var.group(), 'wavelength')
                changed_times = parse_history(getattr(wavelength_var, 'change_history', None))
                changed_times = {t: [
                    finite_or_none(wl) for wl in v.split(',')]
                    for t, v in changed_times.items()
                }
                latest_time = time_coverage_start
                wavelength_values = dict()
                for next_time in sorted(changed_times.keys()):
                    add = changed_times[next_time]
                    if len(add) < wavelength_var.shape[0]:
                        add = add + [None] * (wavelength_var.shape[0] - len(add))
                    elif len(add) > wavelength_var.shape[0]:
                        add = add[:wavelength_var.shape[0]]
                    wavelength_values[latest_time] = add
                    latest_time = next_time
                wavelength_values[latest_time] = [finite_or_none(wl) for wl in wavelength_var[:]]

            if cut_size_idx is None:
                values = values.reshape((*values.shape, 1))
                cut_size_idx = len(values.shape) - 1
                cut_size_values = [None]
            else:
                _, cut_size_var = find_dimension_values(var.group(), 'cut_size')
                cut_size_values = [finite_or_none(wl) for wl in cut_size_var[:]]

            remaining_dimensions = set(range(len(values.shape)))
            remaining_dimensions.remove(0)
            remaining_dimensions.remove(cut_size_idx)
            remaining_dimensions.remove(wavelength_idx)
            remaining_dimensions = sorted(remaining_dimensions)

            # Order for output (cut_size, wavelength, time)
            values = np.transpose(values, (cut_size_idx, wavelength_idx, 0, *remaining_dimensions))
            # Remove extra dimensions
            values = values.reshape(values.shape[:3])

            wavelength_start_time = sorted(wavelength_values.keys())
            for wl_segment_idx in range(len(wavelength_start_time)):
                start_time = wavelength_start_time[wl_segment_idx]
                if wl_segment_idx + 1 < len(wavelength_start_time):
                    end_time = wavelength_start_time[wl_segment_idx + 1]
                else:
                    end_time = MAX_I64
                source_wavelengths = wavelength_values[start_time]

                selected = selected_time_range(times, start_time, end_time)
                if selected is None:
                    continue
                source_time_select = slice(*selected)

                output_data = np.full((
                    len(self._cut_sizes),
                    len(self._wavelengths),
                    selected[1] - selected[0],
                ), nan, dtype=np.float64)

                for cut_size_source_idx in range(len(cut_size_values)):
                    cut_size = cut_size_values[cut_size_source_idx]
                    try:
                        cut_size_dest_idx = self._cut_sizes.index(cut_size)
                    except ValueError:
                        cut_size_dest_idx = len(self._cut_sizes)
                        self._cut_sizes.append(cut_size)
                        self.values = np.pad(self.values, ((0, 1), (0, 0), (0, 0)),
                                             mode='constant', constant_values=nan)
                        output_data = np.pad(output_data, ((0, 1), (0, 0), (0, 0)),
                                             mode='constant', constant_values=nan)

                    for wavelength_source_idx in range(len(source_wavelengths)):
                        wavelength = source_wavelengths[wavelength_source_idx]
                        try:
                            wavelength_dest_idx = self._wavelengths.index(wavelength)
                        except ValueError:
                            wavelength_dest_idx = len(self._wavelengths)
                            self._wavelengths.append(wavelength)
                            self.values = np.pad(self.values, ((0, 0), (0, 1), (0, 0)),
                                                 mode='constant', constant_values=nan)
                            output_data = np.pad(output_data, ((0, 0), (0, 1), (0, 0)),
                                                 mode='constant', constant_values=nan)

                        output_data[cut_size_dest_idx, wavelength_dest_idx, :] = values[
                            cut_size_source_idx, wavelength_source_idx, source_time_select
                        ]

                self.times = np.concatenate((self.times, times[source_time_select]))
                self.values = np.concatenate((self.values, output_data), axis=2)

        def integrate_selected(
                self, root: netCDF4.Dataset,
                *selections: typing.Union[typing.Dict, VariableSelection, str],
                statistics: typing.Optional[str] = None,
        ) -> None:
            for var in VariableSelection.find_matching_variables(root, *selections, statistics=statistics):
                self.integrate_variable(root, var)

        @property
        def has_data(self) -> bool:
            return self.times.shape[0] > 0

        async def write(self, group: netCDF4.Group, output_times: np.ndarray, name: str) -> typing.Optional[netCDF4.Variable]:
            if self.times.shape[0] == 0:
                return None

            def get_existing_dimension(name: str) -> typing.Optional[typing.List[typing.Optional[float]]]:
                var = group.variables.get(name)
                if var is None:
                    return None
                data = var[:].data.astype(np.float64, copy=False).tolist()
                for i in range(len(data)):
                    data[i] = float(data[i])
                    if not isfinite(data[i]):
                        data[i] = None
                return data

            def sorted_dimension(raw: typing.List[typing.Optional[float]]) -> typing.List[typing.Optional[float]]:
                result = list(raw)
                try:
                    result.remove(None)
                    has_none = True
                except ValueError:
                    has_none = False
                result.sort()
                if has_none:
                    result.append(None)
                return result

            target_cut_sizes = get_existing_dimension("cut_size")
            target_wavelengths = get_existing_dimension("wavelength")

            if target_cut_sizes is None and self._cut_sizes != [None]:
                from forge.data.structure.variable import variable_cutsize
                target_cut_sizes = sorted_dimension(self._cut_sizes)
                group.createDimension("cut_size", len(target_cut_sizes))
                var = group.createVariable("cut_size", "f8", ("cut_size",))
                variable_cutsize(var)
                var.coverage_content_type = "coordinate"
                var[:] = [(v if v is not None else nan) for v in target_cut_sizes]

            if target_wavelengths is None and self._wavelengths != [None]:
                from forge.data.structure.variable import variable_wavelength
                target_wavelengths = sorted_dimension(self._wavelengths)
                group.createDimension("wavelength", len(target_wavelengths))
                var = group.createVariable("wavelength", "f8", ("wavelength",))
                variable_wavelength(var)
                var.coverage_content_type = "coordinate"
                var[:] = [(v if v is not None else nan) for v in target_wavelengths]

            def align_dimension(
                    values: np.ndarray, dimension_index: int,
                    source_dimension_values: typing.List[typing.Optional[float]],
                    target_dimension_values: typing.Optional[typing.List[typing.Optional[float]]],
            ) -> np.ndarray:
                if source_dimension_values == [None]:
                    return values
                if source_dimension_values == target_dimension_values:
                    return values
                assert target_dimension_values is not None

                indices: typing.List[typing.Optional[int]] = list()
                for v in target_dimension_values:
                    try:
                        indices.append(source_dimension_values.index(v))
                    except ValueError:
                        indices.append(None)

                modified_shape = list(values.shape)
                modified_shape[dimension_index] = len(indices)
                result = np.full(modified_shape, nan, dtype=np.float64)
                input_index: typing.List[typing.Union[int, slice]] = [slice(None)] * len(modified_shape)
                output_index: typing.List[typing.Union[int, slice]] = [slice(None)] * len(modified_shape)
                for dest_idx in range(len(indices)):
                    source_idx = indices[dest_idx]
                    if source_idx is None:
                        continue
                    input_index[dimension_index] = source_idx
                    output_index[dimension_index] = dest_idx
                    result[tuple(output_index)] = values[tuple(input_index)]
                return result

            def align_data():
                from forge.data.merge.timealign import incoming_before
                indices = incoming_before(output_times, self.times, sort_incoming=True)
                output_values = self.values[..., indices]

                output_values = align_dimension(output_values, 0, self._cut_sizes, target_cut_sizes)
                output_values = align_dimension(output_values, 1, self._wavelengths, target_wavelengths)

                return output_values

            output_values = await asyncio.get_event_loop().run_in_executor(None, align_data)

            var = group.createVariable(name, 'f8', tuple(
                (["cut_size"] if self._cut_sizes != [None] else []) +
                (["wavelength"] if self._wavelengths != [None] else []) +
                ["time"]
            ))

            target_shape = list(output_values.shape)
            if self._wavelengths == [None]:
                del target_shape[1]
            if self._cut_sizes == [None]:
                del target_shape[0]
            target_shape = tuple(target_shape)
            if target_shape != output_values.shape:
                output_values = output_values.reshape(target_shape)

            var[:] = output_values

            return var

    class OutputFile:
        def __init__(self, root: netCDF4.Dataset, times: np.ndarray):
            self.root = root
            self.times = times

            data = self.root.createGroup("data")
            data.createDimension("time")
            var = data.createVariable("time", 'f8', ("time",))
            var.long_name = "start time of measurement"
            var.standard_name = "time"
            var.coverage_content_type = "coordinate"
            var.units = "seconds since 1970-01-01 00:00:00"
            var[:] = times / 1000.0

        def _declare_stp(self) -> None:
            from forge.data.structure.stp import standard_pressure, standard_temperature
            g = self.root.groups["data"]
            standard_pressure(g, change_value=False)
            standard_temperature(g, change_value=False)

        async def instrument(self, instrument: "NCEIFile.MergeInstrument", name: str) -> netCDF4.Group:
            g = self.root.createGroup("data/" + name)
            instrument.write(g)
            await asyncio.sleep(0)
            return g

        async def variable(self, group: netCDF4.Group, name: str, var: "NCEIFile.MergeVariable",
                           attributes: typing.Dict[str, str] = None,
                           is_stp: bool = False) -> typing.Optional[netCDF4.Variable]:
            out_var = await var.write(group, self.times, name)
            if out_var is None:
                return None
            if attributes:
                for attr, value in attributes.items():
                    out_var.setncattr(attr, value)
            if is_stp:
                self._declare_stp()
                ancillary_variables = set(getattr(out_var, 'ancillary_variables', "").split())
                ancillary_variables.add('standard_temperature')
                ancillary_variables.add('standard_pressure')
                out_var.ancillary_variables = " ".join(sorted(ancillary_variables))
            return out_var

    def merge_variable_times(self, *variables: "NCEIFile.MergeVariable") -> np.ndarray:
        from forge.data.merge.timealign import peer_output_time

        return peer_output_time(*[
            v.times for v in variables
        ], apply_rounding=int(round(self.time_coverage_resolution * 1000)))

    def output_file(self, output_directory: Path, *variables: "NCEIFile.MergeVariable"):
        file_creation_time = time.time()

        class Context:
            def __init__(self, file: "NCEIFile"):
                self._file = file
                self._root: typing.Optional[netCDF4.Dataset] = None

            async def __aenter__(self):
                for var in variables:
                    if var.has_data:
                        break
                else:
                    return None

                filename = f"{self._file.file_root_name}_{self._file.station.upper()}_s{format_iso8601_time(self._file.start_epoch_ms / 1000, False)}_e{format_iso8601_time(self._file.end_epoch_ms / 1000, False)}_c{format_iso8601_time(file_creation_time, False)}.nc"
                self._root = netCDF4.Dataset(str(output_directory / filename), 'w', format='NETCDF4')
                self._file.apply_file_metadata(self._root, file_creation_time)

                times = await asyncio.get_event_loop().run_in_executor(
                    None, self._file.merge_variable_times,
                    *variables
                )
                return self._file.OutputFile(self._root, times)

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                if self._root is not None:
                    self._root.close()
                    self._root = None

        return Context(self)

    @abstractmethod
    async def __call__(self, output_directory: Path) -> None:
        pass