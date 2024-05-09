import typing
import asyncio
import logging
import time
import re
from math import floor, ceil
from tempfile import NamedTemporaryFile
from netCDF4 import Dataset
from forge.timeparse import parse_iso8601_time
from forge.formattime import format_iso8601_time
from forge.logicaltime import year_bounds_ms, start_of_year_ms, containing_year_range
from forge.const import STATIONS
from forge.archive.client import data_lock_key, data_file_name, data_notification_key, index_lock_key, index_file_name, event_log_lock_key, event_log_file_name
from forge.archive.client.connection import Connection
from forge.archive.client.archiveindex import ArchiveIndex
from forge.data.merge.instrument import MergeInstrument
from forge.data.merge.eventlog import MergeEventLog
from forge.data.structure.timeseries import file_id, date_created

_LOGGER = logging.getLogger(__name__)
_VALID_INSTRUMENT = re.compile(r"[A-Z][A-Z0-9]*")


class InvalidFile(Exception):
    pass


class ArchivePut:
    def __init__(self, connection: Connection):
        self._connection = connection
        self._index: typing.Dict[str, typing.Dict[str, typing.Dict[int, ArchiveIndex]]] = dict()
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

    def _get_index(self, station: str, archive: str, file_start_time: int) -> ArchiveIndex:
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
            year_index = ArchiveIndex()
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

    async def data(self, file: Dataset, archive: str = "raw",
                   file_start_ms: int = None, file_end_ms: int = None,
                   station: str = None) -> None:
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

    async def _compare_data_file(self, incoming: Dataset, station: str, archive: str, instrument_id: str,
                                 archive_file_start: int, archive_file_end: int,
                                 replace_existing: typing.Callable[[typing.Optional[Dataset], Dataset], bool]) -> bool:
        with NamedTemporaryFile(suffix=".nc") as existing_file:
            archive_file_name = data_file_name(station, archive, instrument_id, archive_file_start / 1000.0)
            try:
                await self._connection.read_file(archive_file_name, existing_file)
                existing_file.flush()
                existing_data = Dataset(existing_file.name, 'r')
                _LOGGER.debug("Comparing existing file for %s", archive_file_name)
                try:
                    return replace_existing(existing_data, incoming)
                finally:
                    existing_data.close()
            except FileNotFoundError:
                _LOGGER.debug("Comparing empty file for %s", archive_file_name)
                return replace_existing(None, incoming)

    async def replace_exact(self, file: Dataset, archive: str = "raw",
                            file_start_ms: int = None, file_end_ms: int = None,
                            station: str = None,
                            replace_existing: typing.Callable[[typing.Optional[Dataset], Dataset], bool] = None) -> None:
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
                archive_file_start, archive_file_end = year_bounds_ms(start_year)
                if destination_end > archive_file_end:
                    raise InvalidFile("Exact file exceeds archive file bounds")

                _LOGGER.debug("Replacing year %d for %s", start_year, instrument_id)
                try:
                    await self._lock_data(station, archive, archive_file_start, archive_file_end)
                    if replace_existing and not self._compare_data_file(
                        file, station, archive, instrument_id, archive_file_start, archive_file_end,
                        replace_existing
                    ):
                        _LOGGER.debug("File comparison rejected")
                        return
                    await self._replace_data_file(file, station, archive, instrument_id,
                                                  archive_file_start, archive_file_end)
                finally:
                    file = None
            else:
                start_day_index = int(floor(destination_start / (24 * 60 * 60 * 1000)))
                archive_file_start = start_day_index * 24 * 60 * 60 * 1000
                archive_file_end = archive_file_start + 24 * 60 * 60 * 1000
                if destination_end > archive_file_end:
                    raise InvalidFile("Exact file exceeds archive file bounds")

                _LOGGER.debug("Replacing day index %d for %s", start_day_index, instrument_id)
                try:
                    await self._lock_data(station, archive, archive_file_start, archive_file_end)
                    if replace_existing and not self._compare_data_file(
                        file, station, archive, instrument_id, archive_file_start, archive_file_end,
                        replace_existing
                    ):
                        _LOGGER.debug("File comparison rejected")
                        return
                    await self._replace_data_file(file, station, archive, instrument_id,
                                                  archive_file_start, archive_file_end)
                finally:
                    file = None

            await self._connection.send_notification(data_notification_key(station, archive),
                                                     destination_start, destination_end)
            _LOGGER.debug("Sent update notification")
        finally:
            if file is not None:
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
