import typing
import asyncio
import logging
import time
import datetime
import re
import netCDF4
import numpy as np
from math import nan, floor, ceil, isfinite
from pathlib import Path
from abc import ABC, abstractmethod
from sqlalchemy.engine import Engine
from forge.database import Database
from forge.logicaltime import year_bounds_ms, round_to_year
from forge.product.selection import InstrumentSelection, VariableSelection
from forge.archive.client.connection import Connection, LockBackoff, LockDenied
from forge.archive.client import index_lock_key, data_lock_key
from forge.data.dimensions import find_dimension_values

_LOGGER = logging.getLogger(__name__)


async def _default_connection() -> Connection:
    return await Connection.default_connection('SQL database update')


class TableUpdate(ABC):
    def __init__(self, station: str, start_epoch_ms: int, end_epoch_ms: int,
                 database_uri: str, password_file: typing.Optional[str] = None):
        self.station = station.lower()
        self.start_epoch_ms = start_epoch_ms
        self.end_epoch_ms = end_epoch_ms
        self.get_archive_connection: typing.Callable[[], typing.Awaitable[Connection]] = _default_connection

        if password_file and '{password}' in database_uri:
            from urllib.parse import quote
            with open(password_file) as f:
                password = f.read()
            database_uri = database_uri.replace('{password}', quote(password))

        self.db = Database(database_uri)

    @staticmethod
    def from_type_code(type_code: str) -> typing.Type["TableUpdate"]:
        from importlib import import_module
        try:
            r = import_module('.' + type_code, 'forge.product.sqldb')
            return r.Update
        except (ModuleNotFoundError, AttributeError):
            raise FileNotFoundError(f"invalid SQL DB output code {type_code}")

    @property
    def archive(self) -> str:
        return "avgh"

    @property
    def table_name(self) -> str:
        raise NotImplementedError

    async def fetch_instrument_files(
            self,
            selections: typing.Iterable[InstrumentSelection],
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
                        for sel in selections:
                            await sel.fetch_files(connection, self.station, self.archive,
                                                  self.start_epoch_ms, self.end_epoch_ms,
                                                  destination_directory)
                    break
                except LockDenied as ld:
                    _LOGGER.debug("Archive busy: %s", ld.status)
                    await backoff()
                    continue

    class VariableColumn:
        class Update:
            def __init__(self, column: "TableUpdate.VariableColumn", times: np.ndarray, values: np.ndarray):
                self.column = column
                self.times = times
                self.values = values

            def __call__(self, value_index: int, time_epoch_ms: int) -> typing.Optional[float]:
                v = float(self.values[value_index])
                if not isfinite(v):
                    return None
                return v

        def __init__(self, name: str,
                     selection: typing.Union[typing.Union[typing.Dict, VariableSelection, str], typing.List[typing.Union[typing.Dict, VariableSelection, str]]],
                     wavelength_index: typing.Optional[int] = None,
                     statistics: typing.Optional[str] = None):
            self.name = name
            self.wavelength_index = wavelength_index
            self.statistics = statistics
            if not isinstance(selection, list):
                selection = [selection]
            self.selections: typing.List[typing.Union[typing.Dict, VariableSelection, str]] = selection

        def variable_index(self, var: netCDF4.Variable) -> typing.Optional[typing.List]:
            idx = [slice(None)] + [0 for _ in range(len(var.shape)-1)]
            if self.wavelength_index is not None:
                try:
                    wl_dim = var.dimensions.index('wavelength')
                except ValueError:
                    return None
                if self.wavelength_index >= var.shape[wl_dim]:
                    return None
                idx[wl_dim] = self.wavelength_index
            return idx

        def __call__(self, root: netCDF4.Dataset) -> typing.Optional["TableUpdate.VariableColumn.Update"]:
            update = None
            for var in VariableSelection.find_matching_variables(root, *self.selections,
                                                                 statistics=self.statistics):
                var_idx = self.variable_index(var)
                if var_idx is None:
                    continue
                values = np.asarray(var[:].data)[tuple(var_idx)]
                if 0 in values.shape:
                    continue
                values = values.astype(np.float64, casting='unsafe', copy=False)

                # Pick the first finite value along each axis to reduce to one dimensional
                while len(values.shape) > 1:
                    if values.shape[1] != 1:
                        valid = np.isfinite(values)
                        idx_valid = np.argmax(valid, axis=1, keepdims=True)
                        values = np.take_along_axis(values, idx_valid, axis=1)

                    target_shape = list(values.shape)
                    del target_shape[1]
                    values = values.reshape(target_shape)

                _, time_var = find_dimension_values(var.group(), 'time')
                times = np.asarray(time_var[:].data)[var_idx[0]]
                times = times.astype(np.int64, casting='unsafe', copy=False)

                assert len(times.shape) == 1
                assert times.shape == values.shape

                if update is None:
                    update = self.Update(self, times, values)
                else:
                    update.times = np.concatenate((update.times, times))
                    update.values = np.concatenate((update.values, values))
            return update

    class _CutSizeVariableColumn(VariableColumn):
        @staticmethod
        def _size_selector(size: np.ndarray) -> np.ndarray:
            raise NotImplementedError

        def variable_index(self, var: netCDF4.Variable) -> typing.Optional[typing.List]:
            idx = super().variable_index(var)
            if idx is None:
                return None
            if 'cut_size' in var.dimensions:
                idx_cut = var.dimensions.index('cut_size')
                _, cut_var = find_dimension_values(var.group(), 'cut_size')
                idx[idx_cut] = self._size_selector(cut_var[:].data)
            else:
                if 'cut_size' not in getattr(var, 'ancillary_variables', "").split():
                    selected = self._size_selector(np.array([nan], dtype=np.float64))
                    if selected.shape[0] != 1 or selected[0] != 0:
                        return None
                    return idx
                cut_var = var.group().variables.get('cut_size')
                if cut_var is None:
                    _LOGGER.warning(f"No sibling cut size variable for {var.name}")
                    return None
                idx[0] = self._size_selector(cut_var[:].data)
            return idx

    class FineCutVariableColumn(_CutSizeVariableColumn):
        @staticmethod
        def _size_selector(size: np.ndarray) -> np.ndarray:
            valid = np.isfinite(size)
            return np.all((
                valid,
                size <= 2.5
            ), axis=0)

    class CoarseCutVariableColumn(_CutSizeVariableColumn):
        @staticmethod
        def _size_selector(size: np.ndarray) -> np.ndarray:
            valid = np.isfinite(size)
            return np.any((
                np.invert(valid),
                size > 2.5
            ), axis=0)

    class KeyColumn:
        def __init__(self, name: str):
            self.name = name
            self.granularity: typing.Optional[int] = None

        @abstractmethod
        def __call__(self, value_index: int, time_epoch_ms: int) -> typing.Any:
            pass

    class DateColumn(KeyColumn):
        def __init__(self, name: str):
            super().__init__(name)
            self.granularity = 24 * 60 * 60 * 1000

        def __call__(self, value_index: int, time_epoch_ms: int) -> typing.Any:
            return datetime.datetime.fromtimestamp(time_epoch_ms / 1000, tz=datetime.timezone.utc).date()

    class TimeColumn(KeyColumn):
        def __init__(self, name: str):
            super().__init__(name)
            self.granularity = 1000

        def __call__(self, value_index: int, time_epoch_ms: int) -> typing.Any:
            return datetime.datetime.fromtimestamp(time_epoch_ms / 1000, tz=datetime.timezone.utc).time()

    class HourColumn(KeyColumn):
        def __init__(self, name: str):
            super().__init__(name)
            self.granularity = 60 * 60 * 1000

        def __call__(self, value_index: int, time_epoch_ms: int) -> typing.Any:
            return datetime.datetime.fromtimestamp(time_epoch_ms / 1000, tz=datetime.timezone.utc).time().hour

    class FractionalYearColumn(KeyColumn):
        def __call__(self, value_index: int, time_epoch_ms: int) -> typing.Any:
            year = time.gmtime(time_epoch_ms / 1000).tm_year
            year_start_ms, year_end_ms = year_bounds_ms(year)
            fraction = (time_epoch_ms - year_start_ms) / (year_end_ms - year_start_ms)
            return year + fraction

    PRIMARY_KEY: typing.List["TableUpdate.KeyColumn"] = None
    EXTRA_KEY: typing.List["TableUpdate.KeyColumn"] = []

    _DAY_FILE_MATCH = re.compile(
        r'[A-Z][0-9A-Z_]{0,31}-[A-Z][A-Z0-9]*_'
        r's((\d{4})(\d{2})(\d{2}))\.nc',
    )

    async def aligned_files(self, file_path: Path) -> typing.AsyncIterable[typing.Tuple[int, int, typing.List[netCDF4.Dataset]]]:
        file_sets: typing.Dict[int, typing.List[Path]] = dict()

        def scan_directory():
            for file in file_path.iterdir():
                if not file.is_file():
                    continue
                match = self._DAY_FILE_MATCH.fullmatch(file.name)
                if not match:
                    continue

                file_epoch_ms = int(floor(datetime.datetime(
                    int(match.group(2)), int(match.group(3)), int(match.group(4)),
                    tzinfo=datetime.timezone.utc
                ).timestamp() * 1000))

                target = file_sets.get(file_epoch_ms)
                if not target:
                    target = []
                    file_sets[file_epoch_ms] = target
                target.append(file)

        await asyncio.get_event_loop().run_in_executor(None, scan_directory)

        for start_epoch_ms in sorted(file_sets.keys()):
            aligned_files = file_sets[start_epoch_ms]
            if self.archive in ("avgm", "avgd"):
                year = time.gmtime(start_epoch_ms / 1000).tm_year
                end_epoch_ms = year_bounds_ms(year)[1]
            else:
                end_epoch_ms = start_epoch_ms + 24 * 60 * 60 * 1000

            open_files = list()
            try:
                for file in aligned_files:
                    open_files.append(netCDF4.Dataset(str(file), 'r'))
                yield start_epoch_ms, end_epoch_ms, open_files
            finally:
                for f in open_files:
                    f.close()
            await asyncio.sleep(0)

    def _remove_inner(self, remove_start: int, remove_end: int, conn) -> None:
        from sqlalchemy.sql import text

        key_by_granularity = list(reversed(sorted(self.PRIMARY_KEY, key=lambda k: k.granularity or 0)))

        parameters: typing.Dict[str, typing.Any] = dict()
        def allocate_parameter(key: TableUpdate.KeyColumn, epoch_ms: int, round_up: bool) -> str:
            if key.granularity:
                if round_up:
                    epoch_ms = int(ceil(epoch_ms / key.granularity) * key.granularity)
                else:
                    epoch_ms = int(floor(epoch_ms / key.granularity) * key.granularity)
            name = f"p{len(parameters)}"
            parameters[name] = key(0, epoch_ms)
            return name

        selectors: typing.List[str] = list()
        start_prefix: str = ""
        end_prefix: str = ""
        for key_idx in range(len(key_by_granularity)):
            key = key_by_granularity[key_idx]
            if key_idx == 0:
                if key_idx + 1 >= len(key_by_granularity):
                    exterior_start = allocate_parameter(key, remove_start, False)
                    exterior_end = allocate_parameter(key, remove_end, True)
                    selectors.append(f"{key.name} >= :{exterior_start} AND {key.name} < :{exterior_end}")
                    break
                interior_start = allocate_parameter(key, remove_start, True)
                interior_end = allocate_parameter(key, remove_end, False)
                selectors.append(f"{key.name} > :{interior_start} AND {key.name} < :{interior_end}")

                edge_start = allocate_parameter(key, remove_start, False)
                edge_end = interior_end
                start_prefix = f"{key.name} = :{edge_start}"
                end_prefix = f"{key.name} = :{edge_end}"
            else:
                if key_idx + 1 >= len(key_by_granularity):
                    exterior_start = allocate_parameter(key, remove_start, False)
                    exterior_end = allocate_parameter(key, remove_end, True)
                    selectors.append(f"({start_prefix}) AND (NOT ({end_prefix})) AND {key.name} >= :{exterior_start}")
                    selectors.append(f"({end_prefix}) AND (NOT ({start_prefix})) AND {key.name} < :{exterior_end}")
                    selectors.append(f"({start_prefix}) AND ({end_prefix}) AND {key.name} >= :{exterior_start} AND {key.name} < :{exterior_end}")
                    break
                interior_start = allocate_parameter(key, remove_start, True)
                interior_end = allocate_parameter(key, remove_end, False)
                selectors.append(f"({start_prefix}) AND (NOT ({end_prefix})) AND {key.name} > :{interior_start}")
                selectors.append(f"({end_prefix}) AND (NOT ({start_prefix})) AND {key.name} < :{interior_end}")

                edge_start = allocate_parameter(key, remove_start, False)
                edge_end = interior_end
                start_prefix += f" AND {key.name} = :{edge_start}"
                end_prefix += f" AND {key.name} = :{edge_end}"

        assert len(selectors) > 0
        conn.execute(text(f"DELETE FROM {self.table_name} WHERE {' OR '.join(['('+s+')' for s in selectors])}"),
                     parameters)

    async def _remove_existing(self, update_start: int, update_end: int) -> None:
        def execute(engine: Engine):
            with engine.begin() as conn:
                self._remove_inner(update_start, update_end, conn)

        begin = time.monotonic()
        await self.db.execute(execute)
        _LOGGER.debug(f"Database removal on {update_start},{update_end} completed in {time.monotonic() - begin:.2f} seconds")

    async def apply_updates(self, update_start: int, update_end: int,
                            updates: typing.List["TableUpdate.VariableColumn.Update"]) -> None:
        update_start = max(update_start, self.start_epoch_ms)
        update_end = min(update_end, self.end_epoch_ms)
        if not updates:
            _LOGGER.debug("No data to update in time range, removing any existing")
            return await self._remove_existing(update_start, update_end)

        def merge_times():
            from forge.data.merge.timealign import peer_output_time
            from forge.data.merge.timeselect import selected_time_range

            merged = peer_output_time(*[
                u.times for u in updates
            ])
            valid_range = selected_time_range(merged, update_start, update_end)
            if valid_range is None:
                return None
            return merged[slice(*valid_range)]

        times = await asyncio.get_event_loop().run_in_executor(None, merge_times)
        if times is None:
            _LOGGER.debug("No target times, removing any existing")
            return await self._remove_existing(update_start, update_end)
        if times[0].shape == 0:
            _LOGGER.debug("No target times, removing any existing")
            return await self._remove_existing(update_start, update_end)

        def align_updates():
            from forge.data.merge.timealign import align_latest

            for u in updates:
                u.values = align_latest(times, u.times, u.values)

        await asyncio.get_event_loop().run_in_executor(None, align_updates)

        def execute(engine: Engine):
            from sqlalchemy.sql import table, column, insert, bindparam

            with engine.begin() as conn:
                self._remove_inner(update_start, update_end, conn)

                param_map: typing.Dict[str, typing.Callable[[int, int], typing.Any]] = dict()

                for key_column in (list(self.PRIMARY_KEY) + list(self.EXTRA_KEY)):
                    param_map[key_column.name] = key_column
                for u in updates:
                    param_map[u.column.name] = u

                table = table(self.table_name, *[column(n) for n in param_map.keys()])
                stmt = insert(table).values(**{
                    n: bindparam(n) for n in param_map.keys()
                })

                params = {}
                for time_idx in range(times.shape[0]):
                    time_epoch_ms = int(times[time_idx])
                    for param_name, get_value in param_map.items():
                        params[param_name] = get_value(time_idx, time_epoch_ms)
                    conn.execute(stmt, params)

        begin = time.monotonic()
        await self.db.execute(execute)
        _LOGGER.debug(f"Database write for {times.shape[0]} times completed in {time.monotonic() - begin:.2f} seconds")

    @abstractmethod
    async def __call__(self) -> None:
        pass