import typing
import logging
import asyncio
import time
import re
import numpy as np
from pathlib import Path
from math import floor, ceil, isfinite, nan
from copy import deepcopy
from abc import ABC, abstractmethod
from netCDF4 import Dataset
from forge.const import MAX_I64
from forge.logicaltime import containing_year_range, start_of_year
from forge.data.merge.timealign import peer_output_time, incoming_before
from forge.data.structure.variable import get_display_units
from forge.vis.data.selection import InstrumentSelection, Selection, FileSource, FileContext, FileSequence, VariableContext
from forge.vis.data.archive import FieldStream, walk_selectable
from forge.archive.client.connection import Connection, LockDenied, LockBackoff
from forge.archive.client.archiveindex import ArchiveIndex
from forge.archive.client import index_lock_key, index_file_name, data_lock_key, data_file_name
from . import Export, ExportList

_LOGGER = logging.getLogger(__name__)


class ArchiveExportEntry(ExportList.Entry):
    def __call__(self, station: str, mode_name: str, export_key: str,
                 start_epoch_ms: int, end_epoch_ms: int, directory: str) -> {typing.Optional[Export]}:
        raise NotImplementedError


class ExportCSV(ArchiveExportEntry):
    class Column:
        def __init__(self, selections: typing.List[Selection], header: str = None, description: str = None,
                     always_present: bool = False, default_header: typing.Optional[str] = None,
                     number_format: str = None, mvc: str = None):
            self.selections = selections
            self.header: typing.Optional[str] = header
            self.description: typing.Optional[str] = description
            self.always_present = always_present
            self.default_header = default_header
            self.number_format = number_format
            self.mvc = mvc

        def __deepcopy__(self, memo):
            y = type(self)(list(self.selections), self.header, self.description,
                           always_present=self.always_present, default_header=self.default_header,
                           number_format=self.number_format, mvc=self.mvc)
            memo[id(self)] = y
            return y

    class Format:
        def __init__(self, cut_size: typing.Optional[bool] = None, limit_time: typing.Optional[int] = -MAX_I64,
                     uniform_time: typing.Optional[int] = -MAX_I64):
            self.cut_size = cut_size
            self.limit_time = limit_time
            self.uniform_time = uniform_time

        def __deepcopy__(self, memo):
            y = type(self)(
                cut_size=self.cut_size,
                limit_time=self.limit_time,
                uniform_time=self.uniform_time,
            )
            memo[id(self)] = y
            return y

    class _RunExport(Export):
        class _OutputColumn(ABC):
            @property
            def enable(self) -> bool:
                return True

            @property
            def header(self) -> str:
                raise NotImplementedError

            @property
            def description(self) -> str:
                raise NotImplementedError

            @abstractmethod
            def __call__(self, row_number: int, epoch_ms: int) -> str:
                pass

        class _DataColumn(_OutputColumn):
            _FORMAT_CODE = re.compile(r'%([- #0+]*)(\d*)(?:\.(\d+))?(?:hh|h|l|ll|q|L|j|z|Z|t)?([diouxXeEfFgG])')

            def __init__(self, input_column: "ExportCSV.Column"):
                super().__init__()
                self.input_column = input_column
                self._instrument_id: typing.Optional[str] = None
                self._header = input_column.header
                self._description = input_column.description
                self._number_format = input_column.number_format
                self._number_convert: typing.Optional[typing.Callable[[np.ndarray], typing.Union[float, int]]] = None
                self._mvc = input_column.mvc or ""
                self._is_state: bool = False
                self._unsorted: bool = False
                self._times: typing.Optional[np.ndarray] = None
                self._values: typing.Optional[np.ndarray] = None
                self._cut_sizes: typing.Optional[np.ndarray] = None

            @property
            def enable(self) -> bool:
                if self.input_column.always_present:
                    return True
                return self._times is not None

            @property
            def header(self) -> str:
                if self._header:
                    return self._header.format(instrument_id=(self._instrument_id or ""))
                return self.input_column.default_header or ""

            @property
            def description(self) -> str:
                return self._description or ""

            def attach(self, chunk_begin: int, instrument_id: str, var: VariableContext,
                       times: typing.Optional[np.ndarray], values: np.ndarray,
                       cut_size_times: typing.Optional[np.ndarray],
                       wavelengths: typing.Optional[np.ndarray]) -> None:
                if instrument_id:
                    self._instrument_id = instrument_id

                if times is None:
                    times = np.ndarray([chunk_begin])
                    values = values.reshape((1, *values.shape))
                else:
                    if len(values.shape) != 1:
                        values = values[tuple([slice(None)] + [0] * (len(values.shape)-1))]

                if self._times is None:
                    self._times = times
                    self._values = values

                    if cut_size_times is not None:
                        self._cut_sizes = cut_size_times
                else:
                    if cut_size_times is not None:
                        if self._cut_sizes is None:
                            self._cut_sizes = np.full(self._values.shape, nan, dtype=np.float32)

                    if int(times[0]) < int(self._times[-1]):
                        self._unsorted = True

                    # Can't use np.concatenate built in casting, since that's Numpy >= 1.20, which can't run on
                    # Python 3.6 (web)
                    self._times = np.concatenate((
                        self._times.astype(np.int64, casting='unsafe', copy=False),
                        times.astype(np.int64, casting='unsafe', copy=False),
                    ))
                    if not np.issubdtype(self._values.dtype, np.floating):
                        self._values = np.concatenate((
                            self._values,
                            values.astype(self._values.dtype, casting='unsafe', copy=False),
                        ))
                    else:
                        self._values = np.concatenate((
                            self._values.astype(np.float32, casting='unsafe', copy=False),
                            values.astype(np.float32, casting='unsafe', copy=False),
                        ))
                    if cut_size_times is not None:
                        self._cut_sizes = np.concatenate((
                            self._cut_sizes.astype(np.float32, casting='unsafe', copy=False),
                            cut_size_times.astype(np.float32, casting='unsafe', copy=False)
                        ))

                if not self._is_state and var.is_state:
                    self._is_state = True

                if not self._description:
                    try:
                        desc = str(var.variable.long_name)
                        if not desc:
                            raise ValueError
                        if wavelengths is not None:
                            add_wl = np.asarray(wavelengths)
                            if add_wl.shape:
                                add_wl = sorted(set([str(int(wl)) for wl in wavelengths]))
                            else:
                                add_wl = [str(int(add_wl))]
                            if add_wl:
                                desc += f" at {','.join(add_wl)} nm"
                        units = get_display_units(var.variable)
                        if units:
                            desc += f" ({units})"
                        self._description = desc
                    except (AttributeError, TypeError, ValueError):
                        pass

                if not self._header:
                    variable_id = var.variable_id
                    if variable_id:
                        if '_' in variable_id or not self._instrument_id:
                            self._header = variable_id
                        else:
                            self._header = variable_id + "_" + self._instrument_id

                if not self._number_format:
                    try:
                        format_code = var.variable.C_format
                        parsed_format = self._FORMAT_CODE.search(format_code)
                        if parsed_format:
                            if '0' not in parsed_format.group(1):
                                format_code = '%0' + parsed_format.group(1) + parsed_format.group(2)
                            else:
                                format_code = '%' + parsed_format.group(1) + parsed_format.group(2)
                            fractional_digits = parsed_format.group(3)
                            if fractional_digits:
                                format_code += '.' + fractional_digits
                            format_code += parsed_format.group(4)
                        if format_code:
                            if np.issubdtype(self._values.dtype, np.floating):
                                _ = format_code % 0.5
                            elif np.issubdtype(self._values.dtype, np.integer):
                                _ = format_code % 1
                            self._number_format = format_code
                    except (AttributeError, TypeError, ValueError):
                        pass

            def align(self, row_times: np.ndarray) -> None:
                if self._values is None:
                    return
                indices = incoming_before(row_times, self._times, sort_incoming=self._unsorted)
                self._times = None
                self._values = self._values[indices]

                if not self._is_state and np.issubdtype(self._values.dtype, np.floating):
                    # A repeat index means a gap: the same value is inserted into multiple rows
                    gap_indices = indices[1:] == indices[:-1]
                    if np.any(gap_indices):
                        modified = self._values[1:]
                        modified[gap_indices] = nan
                        self._values[1:] = modified

                if self._cut_sizes is not None:
                    self._cut_sizes = self._cut_sizes[indices]

                if self._number_format:
                    try:
                        _ = self._number_format % 0.5
                        self._number_convert = float
                    except:
                        try:
                            _ = self._number_format % 1
                            self._number_convert = lambda x: int(round(x))
                        except:
                            self._number_format = None

            def __call__(self, row_number: int, epoch_ms: int) -> str:
                if self._values is None:
                    return self._mvc
                value = self._values[row_number]
                if np.issubdtype(self._values.dtype, np.floating):
                    if not isfinite(value):
                        return self._mvc
                if not self._number_format:
                    return str(value)
                return self._number_format % self._number_convert(value)

            def active_cut_size(self, row_number: int) -> typing.Optional[float]:
                if self._cut_sizes is None:
                    return None
                return float(self._cut_sizes[row_number])

            @property
            def possible_cut_sizes(self) -> typing.Optional[np.ndarray]:
                return self._cut_sizes

            @property
            def value_time(self) -> typing.Optional[np.ndarray]:
                if self._is_state:
                    return None
                return self._times

            @property
            def state_time(self) -> typing.Optional[np.ndarray]:
                if not self._is_state:
                    return None
                return self._times

        class _DataTimeColumn(_OutputColumn):
            @property
            def header(self) -> str:
                return "DateTimeUTC"

            @property
            def description(self) -> str:
                return "Date String (YYYY-MM-DD hh:mm:ss) UTC"

            def __call__(self, row_number: int, epoch_ms: int) -> str:
                ts = time.gmtime(epoch_ms / 1000.0)
                return f"{ts.tm_year:04}-{ts.tm_mon:02}-{ts.tm_mday:02} {ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}"

        class _CutSizeColumn(_OutputColumn):
            def __init__(self, automatic: bool = True):
                self.automatic = automatic
                self.row_sizes: typing.Set[float] = set()
                self._seen: typing.Set[float] = set()

            @property
            def enable(self) -> bool:
                if self.automatic and len(self._seen) <= 1:
                    return False
                return True

            @property
            def header(self) -> str:
                return "Size"

            @property
            def description(self) -> str:
                return "Semicolon delimited list of cut sizes present"

            def __call__(self, row_number: int, epoch_ms: int) -> str:
                row_contents: typing.Set[str] = set()
                for size in self.row_sizes:
                    add = self._map_name(size)
                    if not add:
                        continue
                    row_contents.add(add)
                return ";".join(sorted(row_contents))

            @staticmethod
            def _map_name(size: float) -> typing.Optional[str]:
                if not isfinite(size):
                    return None
                if size < 2.5:
                    return "PM1"
                elif size < 10.0:
                    return "PM2.5"
                return "PM10"

            def integrate(self, cut_sizes: np.ndarray) -> None:
                for size in np.unique(np.floor(cut_sizes / 10) * 10):
                    self._seen.add(float(size))

        def __init__(self, station: str, archive: str, start_epoch_ms: int, end_epoch_ms: int,
                     destination: Path, columns: typing.List["ExportCSV.Column"],
                     format: "ExportCSV.Format"):
            self.station = station
            self.archive = archive
            self.start_epoch_ms = start_epoch_ms
            self.end_epoch_ms = end_epoch_ms
            self.destination = destination
            self.format = format

            if format.limit_time is not None:
                limit_ms = format.limit_time
                if limit_ms < 0 and self.archive not in ("avgh", "avgd", "avgm"):
                    limit_ms = 10 * 366 * 24 * 60 * 60 * 1000
                if limit_ms > 0 and self.end_epoch_ms - self.start_epoch_ms > limit_ms:
                    self.end_epoch_ms = self.start_epoch_ms + limit_ms

            self._columns: typing.List["ExportCSV._RunExport._OutputColumn"] = list()

            self._columns.append(self._DataTimeColumn())

            self._cut_size: typing.Optional["ExportCSV._RunExport._CutSizeColumn"] = None
            if format.cut_size is not None:
                if format.cut_size:
                    c = self._CutSizeColumn(automatic=False)
                    self._cut_size = c
                    self._columns.append(c)
            elif self.archive not in ("avgh", "avgd", "avgm"):
                c = self._CutSizeColumn()
                self._cut_size = c
                self._columns.append(c)

            self._selection_dispatch: typing.Dict[InstrumentSelection, typing.List["ExportCSV._RunExport._DataColumn"]] = dict()
            self._data_columns: typing.List["ExportCSV._RunExport._DataColumn"] = list()
            for input_column in columns:
                c = self._DataColumn(input_column)
                self._columns.append(c)
                self._data_columns.append(c)
                for sel in input_column.selections:
                    dest = self._selection_dispatch.get(sel)
                    if dest is None:
                        dest = list()
                        self._selection_dispatch[sel] = dest
                    dest.append(c)

            self.files = FileSequence(self._selection_dispatch.keys(), station, archive, start_epoch_ms, end_epoch_ms)
            self._round_interval: int = 0

        def _merge_columns(self) -> bool:
            if self._cut_size:
                for c in self._data_columns:
                    add = c.possible_cut_sizes
                    if add is None:
                        continue
                    self._cut_size.integrate(add)

            for i in reversed(range(len(self._columns))):
                c = self._columns[i]
                if not c.enable:
                    if c == self._cut_size:
                        self._cut_size = None
                    del self._columns[i]

            uniform_time: int = 0
            if self.format.uniform_time is not None:
                uniform_time = self.format.uniform_time
                if uniform_time < 0:
                    if self.archive == "avgh":
                        uniform_time = 60 * 60 * 1000
                    elif self.archive == "avgd":
                        uniform_time = 24 * 60 * 60 * 1000
            if uniform_time > 0:
                row_times = np.arange(
                    floor(self.start_epoch_ms / uniform_time) * uniform_time,
                    ceil(self.end_epoch_ms / uniform_time) * uniform_time,
                    uniform_time,
                    dtype=np.int64
                )
            else:
                merge_times: typing.List[np.ndarray] = list()
                for c in self._data_columns:
                    add = c.value_time
                    if add is not None:
                        merge_times.append(add)
                if not merge_times:
                    for c in self._data_columns:
                        add = c.state_time
                        if add is not None:
                            merge_times.append(add)
                if not merge_times:
                    _LOGGER.debug("CSV export has no available times")
                    return False
                row_times = peer_output_time(*merge_times)

            _LOGGER.debug("CSV export merging %d columns with %d rows", len(self._columns), row_times.shape[0])
            if not self._columns or row_times.shape[0] <= 0:
                return False

            for c in self._data_columns:
                c.align(row_times)

            ts = time.gmtime(self.start_epoch_ms / 1000.0)
            output_path = self.destination / f"{self.station.lower()}_{ts.tm_year:04}{ts.tm_mon:02}{ts.tm_mday:02}.csv"
            with output_path.open("wb") as output_file:
                output_file.write((",".join([c.description for c in self._columns])).encode("utf-8"))
                output_file.write(b"\n")
                output_file.write((",".join([c.header for c in self._columns])).encode("utf-8"))
                output_file.write(b"\n")

                for row_index in range(row_times.shape[0]):
                    row_time = int(row_times[row_index])
                    if self._cut_size:
                        active_sizes: typing.Set[float] = set()
                        for c in self._data_columns:
                            add = c.active_cut_size(row_index)
                            if add is not None:
                                active_sizes.add(add)
                        self._cut_size.row_sizes = active_sizes

                    output_file.write(b",".join([c(row_index, row_time).encode('utf-8') for c in self._columns]))
                    output_file.write(b"\n")

            return True

        async def __call__(self) -> typing.Optional[Export.Result]:
            async with await Connection.default_connection("export data", use_environ=False) as connection:
                backoff = LockBackoff()
                while True:
                    try:
                        async with connection.transaction():
                            await self.files.acquire_locks(connection)
                            async for chunk_begin, chunk_files in self.files.run(connection):
                                for src, file_selections in chunk_files.items():
                                    for file, selections in file_selections:
                                        instrument_id = getattr(file, 'instrument_id', None)
                                        for var in walk_selectable(file):
                                            for sel in selections:  # type: Selection
                                                hit = sel.variable_values(
                                                    var,
                                                    return_cut_size_times=self._cut_size is not None,
                                                    return_wavelength=True
                                                )
                                                if hit is None:
                                                    continue
                                                times, values, wavelengths, *cut_size = hit
                                                for col in self._selection_dispatch[sel]:
                                                    col.attach(
                                                        chunk_begin, instrument_id, var, times, values,
                                                        (cut_size[0] if cut_size else None), wavelengths
                                                    )
                        break
                    except LockDenied as ld:
                        _LOGGER.debug("Export data busy: %s", ld.status)
                        await backoff()

            # Can't cancel this since it needs the output directory to continue to exist
            mt = asyncio.ensure_future(asyncio.get_event_loop().run_in_executor(None, self._merge_columns))
            try:
                if not await asyncio.shield(mt):
                    return None
            except asyncio.CancelledError:
                await mt
                raise
            return Export.Result()

    def __init__(self, key: str, display: str, columns: typing.List["ExportCSV.Column"],
                 format: typing.Optional["ExportCSV.Format"] = None):
        super().__init__(key, display)
        self.columns = columns
        self.format = format if format else self.Format()

    def __deepcopy__(self, memo):
        y = type(self)(self.key, self.display, deepcopy(self.columns), deepcopy(self.format))
        memo[id(self)] = y
        return y

    def __call__(self, station: str, mode_name: str, export_key: str,
                 start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        components = mode_name.split('-', 2)
        archive = "raw"
        if len(components) >= 2:
            archive = components[1]
            if archive == "editing":
                archive = "edited"
        return self._RunExport(station, archive, start_epoch_ms, end_epoch_ms, Path(directory),
                               self.columns, self.format)


class ExportNetCDF(ArchiveExportEntry):
    class _RunExport(Export):
        def __init__(
                self,
                station: str, archive: str, start_epoch_ms: int, end_epoch_ms: int,
                destination: Path,
                selections: typing.Optional[typing.List[InstrumentSelection]] = None,
        ):
            self.station = station
            self.archive = archive
            self.start_epoch_ms = start_epoch_ms
            self.end_epoch_ms = end_epoch_ms
            self.destination = destination
            self.selections = selections if selections else [InstrumentSelection()]
            self._unique_archives: typing.Set[str] = set()

        async def _fetch_instrument_file(self, connection: Connection, source: FileSource,
                                         instrument_id: str, file_time: float,
                                         selections: typing.List[InstrumentSelection]) -> None:
            archive_name = data_file_name(source.station, source.archive, instrument_id, file_time)

            ts = time.gmtime(int(file_time))
            if len(self._unique_archives) > 1:
                local_file = self.destination / f"{source.station.upper()}-{instrument_id}-{source.archive.upper()}_s{ts.tm_year:04}{ts.tm_mon:02}{ts.tm_mday:02}.nc"
            else:
                local_file = self.destination / f"{source.station.upper()}-{instrument_id}_s{ts.tm_year:04}{ts.tm_mon:02}{ts.tm_mday:02}.nc"

            try:
                with local_file.open("wb") as f:
                    await connection.read_file(archive_name, f)
            except FileNotFoundError:
                try:
                    local_file.unlink()
                except:
                    pass
                return

            check_file = Dataset(str(local_file), "r")
            try:
                check_context = FileContext(instrument_id, check_file)
                for sel in selections:
                    if sel.accept_file(check_context):
                        local_file = None
                        return
            finally:
                check_file.close()
                if local_file:
                    try:
                        local_file.unlink()
                    except:
                        pass

        async def _fetch_year(self, connection: Connection, year: int, source: FileSource,
                              selections: typing.Optional[typing.List[InstrumentSelection]]) -> None:
            year_start_time = start_of_year(year)
            year_end_time = start_of_year(year + 1)
            lock_start = max(int(floor(year_start_time * 1000)), self.start_epoch_ms)
            lock_end = min(int(ceil(year_end_time * 1000)), self.end_epoch_ms)
            await connection.lock_read(index_lock_key(source.station, source.archive), lock_start, lock_end)
            try:
                index_contents = await connection.read_bytes(
                    index_file_name(source.station, source.archive, year_start_time)
                )
            except FileNotFoundError:
                return
            archive_index = ArchiveIndex(index_contents)

            instrument_selections: typing.Dict[str, typing.List[InstrumentSelection]] = dict()
            for sel in selections:
                sel_instruments = sel.index_to_instruments(archive_index)
                if not sel_instruments:
                    continue
                for instrument_id in sel_instruments:
                    destination = instrument_selections.get(instrument_id)
                    if not destination:
                        destination = list()
                        instrument_selections[instrument_id] = destination
                    destination.append(sel)
            if not instrument_selections:
                return

            await connection.lock_read(data_lock_key(source.station, source.archive), lock_start, lock_end)

            for instrument_id, active_selections in instrument_selections.items():
                if source.archive in ("avgd", "avgm"):
                    await self._fetch_instrument_file(connection, source, instrument_id,
                                                      year_start_time, active_selections)
                    continue

                day_start_range = int(floor(self.start_epoch_ms / (24 * 60 * 60 * 1000))) * 24 * 60 * 60
                day_start_range = max(day_start_range, year_start_time)
                day_end_range = int(ceil(self.end_epoch_ms / (24 * 60 * 60 * 1000))) * 24 * 60 * 60
                day_end_range = min(day_end_range, year_end_time)

                for day_start in range(day_start_range, day_end_range, 24 * 60 * 60):
                    await self._fetch_instrument_file(connection, source, instrument_id,
                                                      day_start, active_selections)

        async def __call__(self) -> typing.Optional[Export.Result]:
            sources: typing.Dict[FileSource, typing.List[InstrumentSelection]] = dict()
            for add_selection in self.selections:
                request_source = add_selection.read_index(self.station, self.archive)
                if not request_source:
                    continue
                destination = sources.get(request_source)
                if not destination:
                    destination = list()
                    sources[request_source] = destination
                destination.append(add_selection)
                self._unique_archives.add(add_selection.archive)

            if not sources:
                return None

            async with await Connection.default_connection("export archive data", use_environ=False) as connection:
                for year in range(*containing_year_range(self.start_epoch_ms / 1000.0, self.end_epoch_ms / 1000.0)):
                    _LOGGER.debug("Exporting NetCDF %s/%s/%d", self.station, self.archive, year)
                    for source, selections in sources.items():
                        backoff = LockBackoff()
                        while True:
                            try:
                                async with connection.transaction():
                                    await self._fetch_year(connection, year, source, selections)
                                break
                            except LockDenied as ld:
                                _LOGGER.debug("Export archive busy: %s", ld.status)
                                await backoff()
            return Export.Result()

    def __init__(self, key: str = None, display: str = None,
                 selections: typing.Optional[typing.List[InstrumentSelection]] = None):
        super().__init__(key or "netcdf", display or "NetCDF4 Archive")
        self.selections = selections

    def __deepcopy__(self, memo):
        y = type(self)(self.key, self.display, list(self.selections) if self.selections is not None else None)
        memo[id(self)] = y
        return y

    def __call__(self, station: str, mode_name: str, export_key: str,
                 start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        components = mode_name.split('-', 2)
        archive = "raw"
        if len(components) >= 2:
            archive = components[1]
            if archive == "editing":
                archive = "edited"
        return self._RunExport(station, archive, start_epoch_ms, end_epoch_ms, Path(directory), self.selections)


class ExportEBAS(ArchiveExportEntry):
    class _RunExport(Export):
        def __init__(
                self,
                station: str, start_epoch_ms: int, end_epoch_ms: int,
                destination: Path, ebas: typing.Set[str],
        ):
            self.station = station
            self.start_epoch_ms = start_epoch_ms
            self.end_epoch_ms = end_epoch_ms
            self.destination = destination
            self.ebas = ebas

            self.end_epoch_ms = min(self.end_epoch_ms, self.start_epoch_ms + 366 * 24 * 60 * 60 * 1000)

        @staticmethod
        async def _archive_connection():
            return await Connection.default_connection("export EBAS data", use_environ=False)

        async def __call__(self) -> typing.Optional[Export.Result]:
            from forge.processing.station.lookup import station_data

            for ebas_type in self.ebas:
                try:
                    converter = station_data(self.station, 'ebas', 'file')(
                        self.station, ebas_type, self.start_epoch_ms, self.end_epoch_ms
                    )
                except FileNotFoundError:
                    _LOGGER.debug(f"EBAS type {ebas_type} not found for {self.station}")
                    continue
                converter = converter(self.station, self.start_epoch_ms, self.end_epoch_ms)
                converter.get_archive_connection = self._archive_connection
                await converter(self.destination)

            return Export.Result()

    def __init__(self, key: str = None, display: str = None,
                 ebas: typing.Iterable[str] = None):
        super().__init__(key or "ebas", display or "EBAS")
        self.ebas = set(ebas) or set()

    def __deepcopy__(self, memo):
        y = type(self)(self.key, self.display, self.ebas)
        memo[id(self)] = y
        return y

    def __call__(self, station: str, mode_name: str, export_key: str,
                 start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
        return self._RunExport(station, start_epoch_ms, end_epoch_ms, Path(directory), self.ebas)
