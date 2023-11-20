import typing
import asyncio
import logging
import time
import datetime
import re
from math import floor, ceil
from json import loads as from_json, dumps as to_json
from tempfile import NamedTemporaryFile
from netCDF4 import Dataset, Variable
from forge.timeparse import parse_iso8601_time
from forge.formattime import format_iso8601_time
from forge.logicaltime import year_bounds_ms, start_of_year_ms, containing_year_range
from forge.const import STATIONS
from forge.archive.client import data_lock_key, data_file_name, data_notification_key, index_lock_key, index_file_name, event_log_lock_key, event_log_file_name
from forge.archive.client.connection import Connection
from forge.data.merge.instrument import MergeInstrument
from forge.data.merge.eventlog import MergeEventLog
from forge.data.structure.timeseries import file_id, date_created

_LOGGER = logging.getLogger(__name__)
_VALID_INSTRUMENT = re.compile(r"[A-Z][A-Z0-9]*")


class InvalidFile(Exception):
    pass


class Index:
    INDEX_VERSION = 1

    def __init__(self):
        self._instrument_codes: typing.Dict[str, typing.Set[str]] = dict()
        self._instrument_tags: typing.Dict[str, typing.Set[str]] = dict()
        self._variable_names: typing.Dict[str, typing.Set[str]] = dict()
        self._standard_names: typing.Dict[str, typing.Set[str]] = dict()
        self._variable_ids: typing.Dict[str, typing.Dict[str, int]] = dict()

    @property
    def known_instrument_ids(self) -> typing.Iterable[str]:
        return self._instrument_tags.keys()

    def integrate_file(self, file: Dataset) -> None:
        instrument_id = file.instrument_id

        add = getattr(file, 'instrument', None)
        if add:
            instrument_codes = self._instrument_codes.get(instrument_id)
            if not instrument_codes:
                instrument_codes = set()
                self._instrument_codes[instrument_id] = instrument_codes
            instrument_codes.add(add)

        instrument_tags = self._instrument_tags.get(instrument_id)
        if not instrument_tags:
            instrument_tags = set()
            self._instrument_tags[instrument_id] = instrument_tags
        instrument_tags.update(str(getattr(file, 'forge_tags', "")).split())

        def recurse_group(group: Dataset) -> None:
            def record_variable_id(var: Variable):
                var_id = getattr(var, 'variable_id', None)
                if var_id is None:
                    return
                var_id = str(var_id)
                if not var_id:
                    return

                variable_info = self._variable_ids.get(var_id)
                if not variable_info:
                    variable_info = dict()
                    self._variable_ids[var_id] = variable_info

                if 'wavelength' in var.dimensions:
                    count = group.dimensions['wavelength'].size
                    variable_info[instrument_id] = max(count, variable_info.get(instrument_id, 0))
                elif 'wavelength' in getattr(var, 'ancillary_variables', "").split():
                    variable_info[instrument_id] = variable_info.get(instrument_id, 1)
                elif var_id not in variable_info:
                    variable_info[instrument_id] = 0

            def record_variable_name(var: Variable):
                name = var.name
                if name in ('time', 'averaged_count', 'averaged_time'):
                    return
                if var.group().parent is None:
                    if name in ('station_name', 'lat', 'lon', 'alt', 'station_inlet_height'):
                        return
                if var.group().name == 'instrument':
                    if name in ('model', 'serial_number', 'firmware_version', 'calibration'):
                        return

                instruments = self._variable_names.get(name)
                if not instruments:
                    instruments = set()
                    self._variable_names[name] = instruments

                instruments.add(instrument_id)

            def record_standard_name_name(var: Variable):
                name = getattr(var, 'standard_name', None)
                if not name:
                    return
                if name == 'time':
                    return
                if var.group().parent is None:
                    if name in ('platform_id', 'latitude', 'longitude', 'altitude', 'height'):
                        return

                instruments = self._standard_names.get(name)
                if not instruments:
                    instruments = set()
                    self._standard_names[name] = instruments

                instruments.add(instrument_id)

            for var in group.variables.values():
                record_variable_id(var)
                record_variable_name(var)
                record_standard_name_name(var)

            for g in group.groups.values():
                recurse_group(g)

        recurse_group(file)

    def integrate_existing(self, contents: bytes) -> None:
        if not contents:
            return
        contents = from_json(contents)
        try:
            version = contents['version']
        except KeyError:
            raise RuntimeError("No index version available")
        if version != self.INDEX_VERSION:
            raise RuntimeError(f"Index version mismatch ({version} vs {self.INDEX_VERSION})")

        def merge_set_lookup(existing: typing.Dict[str, typing.List[str]],
                             destination: typing.Dict[str, typing.Set[str]]):
            for key, values in existing.items():
                target = destination.get(key)
                if not target:
                    target = set()
                    destination[key] = target
                target.update(values)

        merge_set_lookup(contents['instrument_codes'], self._instrument_codes)
        merge_set_lookup(contents['instrument_tags'], self._instrument_tags)
        merge_set_lookup(contents['variable_names'], self._variable_names)
        merge_set_lookup(contents['standard_names'], self._standard_names)

        for var_id, instrument_count in contents['variable_ids'].items():
            target = self._variable_ids.get(var_id)
            if not target:
                self._variable_ids[var_id] = instrument_count
                continue
            for instrument, count in instrument_count.items():
                target[instrument] = max(count, target.get(instrument, 0))

    def commit(self) -> bytes:
        result = {
            'version': self.INDEX_VERSION,
            'variable_ids': self._variable_ids,
        }

        def apply_set_lookup(result_key: str, source: typing.Dict[str, typing.Set[str]]):
            output_value = dict()
            for key, values in source.items():
                output_value[key] = sorted(values)
            result[result_key] = output_value

        apply_set_lookup('instrument_codes', self._instrument_codes)
        apply_set_lookup('instrument_tags', self._instrument_tags)
        apply_set_lookup('variable_names', self._variable_names)
        apply_set_lookup('standard_names', self._standard_names)

        return to_json(result, sort_keys=True).encode('ascii')


class ArchivePut:
    def __init__(self, connection: Connection):
        self._connection = connection
        self._index: typing.Dict[str, typing.Dict[str, typing.Dict[int, Index]]] = dict()
        self._data_locks: typing.Dict[str, typing.Dict[str, typing.Set[int]]] = dict()
        self._eventlog_locks: typing.Dict[str, typing.Set[int]] = dict()

    @staticmethod
    def get_station(file: Dataset, station: str = None) -> str:
        if station is None:
            station_name = file.variables.get("station_name")
            if station_name is not None:
                station = str(station_name[0])

        if not station:
            raise InvalidFile("No station set")

        station = station.lower()
        if station not in STATIONS:
            raise InvalidFile(f"Station {station.upper()} invalid")
        return station

    @staticmethod
    def _get_destination_bounds(file: Dataset,
                                file_start_ms: int = None, file_end_ms: int = None) -> typing.Tuple[int, int]:
        destination_start = file_start_ms
        destination_end = file_end_ms
        if destination_start is None:
            time_coverage_start = getattr(file, 'time_coverage_start', None)
            if time_coverage_start is not None:
                destination_start = int(floor(parse_iso8601_time(str(time_coverage_start)).timestamp() * 1000.0))
        if destination_end is None:
            time_coverage_end = getattr(file, 'time_coverage_end', None)
            if time_coverage_end is not None:
                destination_end = int(ceil(parse_iso8601_time(str(time_coverage_end)).timestamp() * 1000.0))

        if destination_start is None and destination_end is not None:
            destination_start = destination_end - 1
        elif destination_end is None and destination_start is not None:
            destination_end = destination_start + 1
        elif destination_start is None:
            assert destination_end is None
            file_creation_time = getattr(file, 'date_created', None)
            if file_creation_time is not None:
                destination_end = int(ceil(parse_iso8601_time(str(file_creation_time)).timestamp() * 1000.0))
                destination_start = destination_end - 1

        if destination_start is None:
            assert destination_end is None
            raise InvalidFile("No time bounds in file, no data to write")
        elif destination_start >= destination_end:
            raise InvalidFile(
                f"File data excluded by time bounds {format_iso8601_time(destination_start / 1000.0)} {format_iso8601_time(destination_end / 1000.0)}")
        assert destination_start < destination_end

        return destination_start, destination_end

    def _get_index(self, station: str, archive: str, file_start_time: int) -> Index:
        station_index = self._index.get(station)
        if not station_index:
            station_index = dict()
            self._index[station] = station_index
        archive_index = station_index.get(archive)
        if not archive_index:
            archive_index = dict()
            station_index[archive] = archive_index

        ts = time.gmtime(int(file_start_time / 1000))
        year_number = ts.tm_year
        year_index = archive_index.get(year_number)
        if not year_index:
            year_index = Index()
            archive_index[year_number] = year_index
        return year_index

    async def _lock_data(self, station: str, archive: str, lock_start: int, lock_end: int) -> None:
        station_locks = self._data_locks.get(station)
        if not station_locks:
            station_locks = dict()
            self._data_locks[station] = station_locks
        archive_locks = station_locks.get(archive)
        if not archive_locks:
            archive_locks = set()
            station_locks[archive] = archive_locks
        if lock_start in archive_locks:
            return
        _LOGGER.debug("Acquiring lock for %s/%s", station, archive)
        await self._connection.lock_write(data_lock_key(station, archive), lock_start, lock_end)
        archive_locks.add(lock_start)

    async def preemptive_lock_range(self, station: str, archive: str, lock_start_ms: int, lock_end_ms: int) -> None:
        station_locks = self._data_locks.get(station)
        if not station_locks:
            station_locks = dict()
            self._data_locks[station] = station_locks
        archive_locks = station_locks.get(archive)
        if not archive_locks:
            archive_locks = set()
            station_locks[archive] = archive_locks

        if archive in ("avgd", "avgm"):
            start_year, end_year = containing_year_range(lock_start_ms / 1000.0, lock_end_ms / 1000.0)
            year_start_ms = start_of_year_ms(start_year)
            year_end_ms = start_of_year_ms(end_year)

            year_lock_times = set()
            for year in range(start_year, end_year):
                add = start_of_year_ms(year)
                year_lock_times.add(add)

            if year_lock_times.issubset(archive_locks):
                return

            _LOGGER.debug("Acquiring lock for %s/%s on %d,%d", station, archive, year_start_ms, year_end_ms)
            await self._connection.lock_write(data_lock_key(station, archive), year_start_ms, year_end_ms)
            archive_locks.update(year_lock_times)
        else:
            lock_start_ms = int(floor(lock_start_ms / (24 * 60 * 60 * 1000))) * (24 * 60 * 60 * 1000)
            lock_end_ms = int(ceil(lock_end_ms / (24 * 60 * 60 * 1000))) * (24 * 60 * 60 * 1000)
            if lock_end_ms <= lock_start_ms:
                lock_end_ms += (24 * 60 * 60 * 1000)

            for check in range(lock_start_ms, lock_end_ms, 24 * 60 * 60 * 1000):
                if check not in archive_locks:
                    break
            else:
                return

            _LOGGER.debug("Acquiring lock for %s/%s on %d,%d", station, archive, lock_start_ms, lock_end_ms)
            await self._connection.lock_write(data_lock_key(station, archive), lock_start_ms, lock_end_ms)
            for lock_day in range(lock_start_ms, lock_end_ms, 24 * 60 * 60 * 1000):
                archive_locks.add(lock_day)

    async def _write_data_file(self, file: Dataset, station: str, archive: str, instrument_id: str,
                               archive_file_start: int, archive_file_end: int,
                               temp_file: typing.Optional[NamedTemporaryFile] = None) -> None:
        now = time.time()
        date_created(file, now)
        file_id(file, instrument_id, archive_file_start / 1000.0, archive_file_end / 1000.0, now)
        file.time_coverage_start = format_iso8601_time(archive_file_start / 1000.0)
        file.time_coverage_end = format_iso8601_time(archive_file_end / 1000.0)

        self._get_index(station, archive, archive_file_start).integrate_file(file)

        source_file = temp_file
        if not temp_file:
            source_file = open(file.filepath(), "rb")
        file.close()
        source_file.seek(0)

    def _prepare_data_file(self, file: Dataset, station: str, archive: str, instrument_id: str,
                           archive_file_start: int, archive_file_end: int) -> None:
        now = time.time()
        date_created(file, now)
        file_id(file, instrument_id, archive_file_start / 1000.0, archive_file_end / 1000.0, now)
        file.time_coverage_start = format_iso8601_time(archive_file_start / 1000.0)
        file.time_coverage_end = format_iso8601_time(archive_file_end / 1000.0)

        self._get_index(station, archive, archive_file_start).integrate_file(file)

    async def _merge_data_file(self, file: Dataset, station: str, archive: str, instrument_id: str,
                               archive_file_start: int, archive_file_end: int) -> None:
        with NamedTemporaryFile(suffix=".nc") as existing_file, NamedTemporaryFile(suffix=".nc") as merged_file:
            merge = MergeInstrument()

            archive_file_name = data_file_name(station, archive, instrument_id, archive_file_start / 1000.0)
            try:
                await self._connection.read_file(archive_file_name, existing_file)
                existing_file.flush()
                existing_data = Dataset(existing_file.name, 'r')
                merge.overlay(existing_data, archive_file_start, archive_file_end)
                _LOGGER.debug("Using existing data file %s", archive_file_name)
            except FileNotFoundError:
                _LOGGER.debug("No existing data for %s", archive_file_name)
                existing_data = None

            try:
                merge.overlay(file, archive_file_start, archive_file_end)
                result = merge.execute(merged_file.name)
            finally:
                if existing_data is not None:
                    existing_data.close()
                    existing_data = None
            try:
                self._prepare_data_file(result, station, archive, instrument_id,
                                        archive_file_start, archive_file_end)
                self._get_index(station, archive, archive_file_start).integrate_file(result)
                result.close()
                result = None
            finally:
                if result is not None:
                    result.close()

            merged_file.seek(0)
            await self._connection.write_file(archive_file_name, merged_file)
            _LOGGER.debug("Sent updated file %s", archive_file_name)

    async def _replace_data_file(self, file: Dataset, station: str, archive: str, instrument_id: str,
                                 archive_file_start: int, archive_file_end: int) -> None:
        try:
            self._prepare_data_file(file, station, archive, instrument_id,
                                    archive_file_start, archive_file_end)
            self._get_index(station, archive, archive_file_start).integrate_file(file)
            source_file_name = file.filepath()
            file.close()
            file = None

            with open(source_file_name, "rb") as source_data:
                archive_file_name = data_file_name(station, archive, instrument_id, archive_file_start / 1000.0)
                await self._connection.write_file(archive_file_name, source_data)
                _LOGGER.debug("Sent replacement file %s", archive_file_name)
        finally:
            if file is not None:
                file.close()

    async def data(self, file: Dataset, archive: str = "raw",
                   file_start_ms: int = None, file_end_ms: int = None,
                   station: str = None, exact_contents: bool = False) -> None:
        try:
            instrument_id = getattr(file, 'instrument_id', None)
            if not instrument_id:
                raise InvalidFile("No instrument identifier in file")
            if not _VALID_INSTRUMENT.fullmatch(instrument_id) or instrument_id.upper() == "LOG":
                raise InvalidFile("Invalid instrument identifier in file")

            archive = archive.lower()
            assert archive in ("raw", "edited", "clean", "avgh", "avgd", "avgm")

            station = self.get_station(file, station)
            destination_start, destination_end = self._get_destination_bounds(file, file_start_ms, file_end_ms)

            if archive in ("avgd", "avgm"):
                start_year = time.gmtime(destination_start / 1000.0).tm_year

                if exact_contents:
                    archive_file_start, archive_file_end = year_bounds_ms(start_year)
                    if destination_end > archive_file_end:
                        raise InvalidFile("Exact file exceeds archive file bounds")

                    _LOGGER.debug("Replacing year %d for %s", start_year, instrument_id)
                    try:
                        await self._replace_data_file(file, station, archive, instrument_id,
                                                      archive_file_start, archive_file_end)
                    finally:
                        file = None
                else:
                    end_year = time.gmtime(destination_end / 1000.0).tm_year + 1

                    for year in range(start_year, end_year):
                        _LOGGER.debug("Processing year %d for %s", year, instrument_id)
                        archive_file_start, archive_file_end = year_bounds_ms(year)

                        if archive_file_start >= destination_end:
                            break

                        await self._lock_data(station, archive, archive_file_start, archive_file_end)
                        await self._merge_data_file(file, station, archive, instrument_id,
                                                    archive_file_start, archive_file_end)
            else:
                start_day_index = int(floor(destination_start / (24 * 60 * 60 * 1000)))
                if exact_contents:
                    archive_file_start = start_day_index * 24 * 60 * 60 * 1000
                    archive_file_end = archive_file_start + 24 * 60 * 60 * 1000
                    if destination_end > archive_file_end:
                        raise InvalidFile("Exact file exceeds archive file bounds")

                    _LOGGER.debug("Replacing day index %d for %s", start_day_index, instrument_id)
                    try:
                        await self._replace_data_file(file, station, archive, instrument_id,
                                                      archive_file_start, archive_file_end)
                    finally:
                        file = None
                else:
                    end_day_index = int(ceil(destination_end / (24 * 60 * 60 * 1000))) + 1

                    for day_index in range(start_day_index, end_day_index):
                        _LOGGER.debug("Processing day index %d for %s", day_index, instrument_id)
                        archive_file_start = day_index * 24 * 60 * 60 * 1000
                        archive_file_end = archive_file_start + 24 * 60 * 60 * 1000

                        if archive_file_start >= destination_end:
                            break

                        await self._lock_data(station, archive, archive_file_start, archive_file_end)
                        await self._merge_data_file(file, station, archive, instrument_id,
                                                    archive_file_start, archive_file_end)

                await self._connection.send_notification(data_notification_key(station, archive),
                                                         destination_start, destination_end)
                _LOGGER.debug("Sent update notification")
        finally:
            if exact_contents and file is not None:
                file.close()

    async def _lock_event_log(self, station: str, lock_start: int, lock_end: int) -> None:
        station_locks = self._eventlog_locks.get(station)
        if not station_locks:
            station_locks = set()
            self._eventlog_locks[station] = station_locks
        if lock_start in station_locks:
            return
        _LOGGER.debug("Acquiring event log lock for %s", station)
        await self._connection.lock_write(event_log_lock_key(station), lock_start, lock_end)
        station_locks.add(lock_start)

    async def event_log(self, file: Dataset, station: str = None) -> None:
        log_group = file.groups.get("log")
        if log_group is None:
            raise InvalidFile
        event_time = log_group.variables.get("time")
        if event_time is None:
            raise InvalidFile
        if len(event_time.shape) != 1:
            raise InvalidFile
        if event_time.shape[0] == 0:
            return

        destination_start = int(event_time[0])
        destination_end = int(event_time[-1])
        station = self.get_station(file, station)

        start_day_index = int(floor(destination_start / (24 * 60 * 60 * 1000)))
        end_day_index = int(ceil((destination_end + 1) / (24 * 60 * 60 * 1000))) + 1

        for day_index in range(start_day_index, end_day_index):
            _LOGGER.debug("Processing events for day index %d", day_index)
            archive_file_start = day_index * 24 * 60 * 60 * 1000
            archive_file_end = archive_file_start + 24 * 60 * 60 * 1000

            if archive_file_start > destination_end:
                break

            await self._lock_event_log(station, archive_file_start, archive_file_end)

            with NamedTemporaryFile(suffix=".nc") as existing_file, NamedTemporaryFile(suffix=".nc") as merged_file:
                merge = MergeEventLog()

                archive_file_name = event_log_file_name(station, archive_file_start / 1000.0)
                try:
                    await self._connection.read_file(archive_file_name, existing_file)
                    existing_file.flush()
                    existing_data = Dataset(existing_file.name, 'r')
                    merge.overlay(existing_data, archive_file_start, archive_file_end)
                    _LOGGER.debug("Using existing data file %s", archive_file_name)
                except FileNotFoundError:
                    _LOGGER.debug("No existing data for %s", archive_file_name)
                    existing_data = None

                merge.overlay(file, archive_file_start, archive_file_end)

                result = merge.execute(merged_file.name)
                if existing_data is not None:
                    existing_data.close()
                    existing_data = None

                if result is None:
                    _LOGGER.debug("Removing empty event log %s", archive_file_name)
                    await self._connection.remove_file(archive_file_name)
                    continue

                now = time.time()
                date_created(result, now)
                file_id(result, "LOG", archive_file_start / 1000.0, archive_file_end / 1000.0, now)
                result.time_coverage_start = format_iso8601_time(archive_file_start / 1000.0)
                result.time_coverage_end = format_iso8601_time(archive_file_end / 1000.0)

                result.close()
                result = None

                merged_file.seek(0)
                await self._connection.write_file(archive_file_name, merged_file)
                _LOGGER.debug("Sent updated file %s", archive_file_name)

    async def auto(self, file: Dataset, station: str = None) -> None:
        if getattr(file, 'forge_tags', None) == 'eventlog':
            await self.event_log(file, station=station)
        else:
            await self.data(file, station=station)

    async def commit_index(self) -> None:
        for station, station_index in self._index.items():
            for archive, archive_index in station_index.items():
                for year, index in archive_index.items():
                    year_start, year_end = year_bounds_ms(year)
                    _LOGGER.debug("Updating index for %s/%s year %d", station, archive, year)
                    await self._connection.lock_write(index_lock_key(station, archive), year_start, year_end)
                    index_file = index_file_name(station, archive, year_start / 1000.0)
                    try:
                        index_contents = await self._connection.read_bytes(index_file)
                        index.integrate_existing(index_contents)
                    except FileNotFoundError:
                        _LOGGER.debug("No index for year found")
                    index_contents = index.commit()
                    await self._connection.write_bytes(index_file, index_contents)


def cli():
    import argparse
    import sys
    from forge.archive import CONFIGURATION
    from forge.archive.client.connection import Connection, LockDenied, LockBackoff

    parser = argparse.ArgumentParser(description="Forge archive file write.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--server-host',
                        dest='tcp_server',
                        help="archive server host")
    group.add_argument('--server-socket',
                        dest='unix_socket',
                        help="archive server Unix socket")
    parser.add_argument('--server-port',
                        dest='tcp_port',
                        type=int,
                        default=CONFIGURATION.get("ARCHIVE.PORT"),
                        help="archive server port")

    parser.add_argument('--station',
                        dest='station',
                        help="override destination station")

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--archive',
                        dest='archive',
                        choices=["raw", "edited", "clean", "avgh", "avgd", "avgm"],
                        help="destination data archive")
    group.add_argument('--event-log',
                        dest='event_log', action='store_true',
                        help="write to the event log")

    parser.add_argument('file',
                        nargs='+')

    args = parser.parse_args()
    if args.tcp_server and not args.tcp_port:
        parser.error("Both a server host and port must be specified")

    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    loop = asyncio.new_event_loop()

    any_valid_files = False

    async def run():
        nonlocal any_valid_files

        if args.tcp_server and args.tcp_port:
            _LOGGER.debug(f"Connecting to archive TCP socket {args.tcp_server}:{args.tcp_port}")
            reader, writer = await asyncio.open_connection(args.tcp_server, int(args.tcp_port))
            connection = Connection(reader, writer, "write archive data")
        elif args.unix_socket:
            _LOGGER.debug(f"Connecting to archive Unix socket {args.unix_socket}")
            reader, writer = await asyncio.open_unix_connection(args.unix_socket)
            connection = Connection(reader, writer, "write archive data")
        else:
            connection = await Connection.default_connection("write archive data")

        await connection.startup()

        backoff = LockBackoff()
        try:
            while True:
                try:
                    async with connection.transaction(True):
                        put = ArchivePut(connection)

                        any_valid_files = False
                        for file_name in args.file:
                            file = Dataset(file_name, 'r')
                            try:
                                try:
                                    if args.event_log or getattr(file, 'forge_tags', None) == 'eventlog':
                                        await put.event_log(file, station=args.station)
                                    else:
                                        await put.data(file, archive=args.archive or "raw", station=args.station)
                                    any_valid_files = True
                                except InvalidFile:
                                    _LOGGER.debug("Invalid file passed", exc_info=True)
                                    if not backoff.has_failed:
                                        print(f"Ignoring invalid file '{file_name}'")
                            finally:
                                file.close()
                                file = None

                        await put.commit_index()
                    break
                except LockDenied as ld:
                    _LOGGER.debug("Archive busy: %s", ld.status)
                    if sys.stdout.isatty():
                        if not backoff.has_failed:
                            sys.stdout.write("\n")
                        sys.stdout.write(f"\x1B[2K\rBusy: {ld.status}")
                    await backoff()
        finally:
            if backoff.has_failed and sys.stdout.isatty():
                sys.stdout.write("\n")

        await connection.shutdown()

    loop.run_until_complete(run())
    loop.close()

    if not any_valid_files:
        print("No valid files processed")
        exit(1)


if __name__ == '__main__':
    cli()
