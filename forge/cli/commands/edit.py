import typing
import asyncio
import logging
import argparse
import time
from pathlib import Path
from math import floor, ceil
from netCDF4 import Dataset
from tempfile import TemporaryDirectory
from forge.logicaltime import containing_year_range, start_of_year
from forge.timeparse import parse_iso8601_time
from forge.archive.client import edit_directives_lock_key, edit_directives_file_name
from forge.archive.client.connection import Connection, LockDenied, LockBackoff
from forge.processing.editing.directives import apply_edit_directives
from ..execute import Execute, ExecuteStage, Progress
from . import ParseCommand, ParseArguments


if typing.TYPE_CHECKING:
    from .get import ArchiveRead

_LOGGER = logging.getLogger(__name__)


class Command(ParseCommand):
    COMMANDS: typing.List[str] = ["edit"]
    HELP: str = "apply station edits and corrections"

    @classmethod
    def available(cls, cmd: ParseArguments.SubCommand, execute: "Execute") -> bool:
        return not cmd.is_last

    @classmethod
    def install(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                parser: argparse.ArgumentParser) -> None:
        if cmd.is_first:
            from .get import Command as GetCommand
            GetCommand.install_pure(cmd, execute, parser)

        group = parser.add_mutually_exclusive_group()
        group.add_argument('--no-correct',
                           dest='correct', action='store_false',
                           help="disable data corrections")
        group.set_defaults(correct=True)
        group.add_argument('--no-mentor',
                           dest='mentor', action='store_false',
                           help="disable mentor edits")
        group.set_defaults(mentor=True)

        parser.add_argument('--edit-station',
                            dest='edit_station',
                            help="override the station used for editing")

    @classmethod
    def instantiate(cls, cmd: ParseArguments.SubCommand, execute: Execute,
                    parser: argparse.ArgumentParser,
                    args: argparse.Namespace, extra_args: typing.List[str]) -> None:
        if cmd.is_first:
            from .get import Command as GetCommand
            read = GetCommand.instantiate_pure(cmd, execute, parser, args, extra_args)
            if len(read.archives) <= 1:
                execute.install(_EditStageArchive(execute, args, read))
                return
        else:
            cls.no_extra_args(parser, extra_args)

        execute.install(_EditStageFreeform(execute, args))


class _EditStage(ExecuteStage):
    def __init__(self, execute: Execute, args: argparse.Namespace):
        super().__init__(execute)
        self.apply_edits: bool = args.mentor
        self.apply_corrections: bool = args.correct
        self.override_station: typing.Optional[str] = args.edit_station.lower() if args.edit_station else None
        self._edit_file_storage: typing.Optional[TemporaryDirectory] = None
        self.edit_files: typing.Dict[str, typing.List[Path]] = dict()

    async def before(self) -> None:
        if self.apply_edits:
            self._edit_file_storage = TemporaryDirectory()

    async def after(self) -> None:
        if self._edit_file_storage:
            self._edit_file_storage.cleanup()
        self._edit_file_storage = None
        self.edit_files.clear()

    async def _get_edit_file(self, connection: Connection, station: str, path: str) -> None:
        output_file = Path(self._edit_file_storage.name) / Path(path).name
        with output_file.open("wb") as f:
            try:
                await connection.read_file(path, f)
                dest = self.edit_files.get(station)
                if dest is None:
                    dest = list()
                    self.edit_files[station] = dest
                dest.append(output_file)
                return
            except FileNotFoundError:
                pass
        try:
            output_file.unlink()
        except (OSError, FileNotFoundError):
            pass

    async def _load_edits(self, connection: Connection, station: str, start_ms: int, end_ms: int,
                          progress: typing.Optional[Progress] = None) -> None:
        _LOGGER.debug("Loading edits for station %s in %d to %d", station, start_ms, end_ms)
        if progress:
            progress.set_title(f"Loading {station.upper()} edits")

        await connection.lock_read(edit_directives_lock_key(station), start_ms, end_ms)

        await self._get_edit_file(connection, station, edit_directives_file_name(station, None))
        for year in range(*containing_year_range(start_ms / 1000.0, end_ms / 1000.0)):
            if progress:
                progress.set_title(f"Loading {station.upper()}/{year} edits")

            year_start = start_of_year(year)
            await self._get_edit_file(connection, station, edit_directives_file_name(station, year_start))


class _EditStageArchive(_EditStage):
    def __init__(self, execute: Execute, args: argparse.Namespace, archive: "ArchiveRead"):
        super().__init__(execute, args)
        self._archive = archive

    def _day_file_iterate(
            self,
            station_files: typing.Dict[int, typing.List[Path]],
    ) -> typing.Iterator[typing.Tuple[int, int, typing.List[Dataset]]]:
        for day_number, day_files in station_files.items():
            open_data_files: typing.List[Dataset] = list()
            try:
                edit_start_ms: int = day_number * 24 * 60 * 60 * 1000
                end_end_ms: int = edit_start_ms
                for file_path in day_files:
                    data_file = Dataset(str(file_path), 'r+')
                    open_data_files.append(data_file)

                    end_time = getattr(data_file, 'time_coverage_end', None)
                    if end_time is not None:
                        end_time = int(ceil(parse_iso8601_time(str(end_time)).timestamp() * 1000))
                        end_end_ms = max(end_end_ms, end_time)
                if end_end_ms <= edit_start_ms:
                    end_end_ms = self._archive.end_ms
                    if end_end_ms <= edit_start_ms:
                        end_end_ms = edit_start_ms + 24 * 60 * 60 * 1000

                yield edit_start_ms, end_end_ms, open_data_files
            finally:
                for file in open_data_files:
                    file.close()

    async def _run_edits(self, stations: typing.Dict[str, typing.Dict[int, typing.List[Path]]]) -> None:
        edit_begin_time = time.monotonic()
        with self.progress("Loading edits") as progress:
            async with await self.exec.archive_connection() as connection:
                for station in self._archive.stations:
                    backoff = LockBackoff()
                    while True:
                        try:
                            async with connection.transaction():
                                await self._load_edits(
                                    connection, station,
                                    self._archive.start_ms, self._archive.end_ms,
                                    progress
                                )
                            break
                        except LockDenied:
                            await backoff()
                            continue
        edit_load_finish = time.monotonic()

        with self.progress("Applying edits") as progress:
            completed_files: int = 0
            total_files: int = 0
            for station_files in stations.values():
                total_files += len(station_files)

            for station, station_files in stations.items():
                _LOGGER.debug("Applying edits for station %s to %d file sets", station, len(station_files))
                progress.set_title(f"Applying {station.upper()} edits")

                station_edit_files = self.edit_files.get(station, None)
                if not station_edit_files:
                    for day_files in station_files.values():
                        completed_files += len(day_files)
                    continue

                directives_files: typing.List[Dataset] = list()
                try:
                    for file in station_edit_files:
                        file = Dataset(str(file), 'r')
                        directives_files.append(file)

                    for edit_start_ms, edit_end_ms, open_data_files in self._day_file_iterate(station_files):
                        progress(completed_files / total_files)
                        for apply_directives in directives_files:
                            apply_edit_directives(apply_directives, open_data_files, edit_start_ms, edit_end_ms)
                        completed_files += len(open_data_files)
                finally:
                    for file in directives_files:
                        file.close()

        edit_end_time = time.monotonic()
        _LOGGER.debug("Edits applied in %.3f seconds, with  %.3f seconds spent loading", edit_end_time - edit_begin_time, edit_load_finish - edit_begin_time)

    async def _run_corrections(self, stations: typing.Dict[str, typing.Dict[int, typing.List[Path]]]) -> None:
        from forge.processing.editing.run import EditingAvailableDay
        from forge.processing.station.lookup import station_data

        correct_begin_time = time.monotonic()

        completed_files: int = 0
        total_files: int = 0
        for station_files in stations.values():
            total_files += len(station_files)

        with self.progress("Applying corrections") as progress:
            for station, station_files in stations.items():
                _LOGGER.debug("Loading corrections for station %s to %d file sets", station, len(station_files))
                progress.set_title(f"Applying {station.upper()} corrections")

                runner = station_data(station, 'editing', 'run')
                for edit_start_ms, edit_end_ms, open_data_files in self._day_file_iterate(station_files):
                    progress(completed_files / total_files)
                    available = EditingAvailableDay(
                        station, self.data_path,
                        int(floor(edit_start_ms / 1000)),
                        list(open_data_files),
                        day_end=int(ceil(edit_end_ms / 1000)),
                    )
                    completed_files += len(open_data_files)
                    open_data_files.clear()
                    runner(available)

        correct_end_time = time.monotonic()
        _LOGGER.debug("Corrections applied in %.3f seconds", correct_end_time - correct_begin_time)

    async def __call__(self) -> None:
        self.ensure_writable()

        scan_begin_time = time.monotonic()
        stations: typing.Dict[str, typing.Dict[int, typing.List[Path]]] = dict()
        for file in self.data_files():
            data_file = Dataset(str(file), 'r+')
            try:
                if self.override_station:
                    station_name = self.override_station
                else:
                    station_name = data_file.variables.get("station_name")
                    if station_name is None:
                        continue
                    station_name = str(station_name[0]).lower()

                start_time = getattr(data_file, 'time_coverage_start', None)
                if start_time is not None:
                    start_time = int(floor(parse_iso8601_time(str(start_time)).timestamp()))
                if not start_time:
                    start_time = 1

                edit_day = int(floor(start_time / (24 * 60 * 60)))

                station_data = stations.get(station_name)
                if station_data is None:
                    stations[station_name] = {edit_day: [file]}
                else:
                    day_files = station_data.get(edit_day)
                    if day_files is None:
                        station_data[edit_day] = [file]
                    else:
                        day_files.append(file)
            finally:
                data_file.close()
        scan_end_time = time.monotonic()
        _LOGGER.debug("Edit file scan completed in %.3f seconds, with %d stations", scan_end_time - scan_begin_time, len(stations))

        if not self.apply_edits:
            await self._run_corrections(stations)
        elif not self.apply_corrections:
            await self._run_edits(stations)
        else:
            await self._run_edits(stations)
            await self._run_corrections(stations)


class _EditStageFreeform(_EditStage):
    async def _run_edits(self, stations: typing.Dict[str, typing.Tuple[int, int, typing.Dict[str, typing.List[Path]]]]) -> None:
        edit_begin_time = time.monotonic()
        with self.progress("Loading edits") as progress:
            async with await self.exec.archive_connection() as connection:
                for station, (start, end, _) in stations.items():
                    backoff = LockBackoff()
                    while True:
                        try:
                            async with connection.transaction():
                                await self._load_edits(connection, station, start, end, progress)
                            break
                        except LockDenied:
                            await backoff()
                            continue
        edit_load_finish = time.monotonic()

        with self.progress("Applying edits") as progress:
            completed_stations: int = 0
            for station, (start, end, archive_file_paths) in stations.items():
                for archive, file_paths in archive_file_paths.items():
                    _LOGGER.debug("Applying %s/%s edits to %d files", station, archive, len(file_paths))
                    progress.set_title(f"Applying {station.upper()} edits")
                    progress(completed_stations / len(stations))

                    station_files: typing.List[Dataset] = list()
                    try:
                        def run_edits():
                            _LOGGER.debug("Applying %s mentor edits to %d files", station, len(station_files))
                            for directives_file in self.edit_files.get(station, []):
                                directives_file = Dataset(directives_file, 'r')
                                try:
                                    apply_edit_directives(directives_file, station_files, start, end)
                                finally:
                                    directives_file.close()

                        for file in file_paths:
                            if len(station_files) > 128:
                                run_edits()
                                for file in station_files:
                                    file.close()
                                station_files.clear()

                            station_files.append(Dataset(str(file), 'r+'))

                        if station_files:
                            run_edits()

                        completed_stations += 1
                    finally:
                        for file in station_files:
                            file.close()

        edit_end_time = time.monotonic()
        _LOGGER.debug("Edits applied in %.3f seconds, with  %.3f seconds spent loading", edit_end_time - edit_begin_time, edit_load_finish - edit_begin_time)

    async def _run_corrections(self, stations: typing.Dict[str, typing.Tuple[int, int, typing.Dict[str, typing.List[Path]]]]) -> None:
        from forge.processing.context.standalone import RunAvailable
        from forge.processing.station.lookup import station_data

        correct_begin_time = time.monotonic()

        with self.progress("Applying corrections") as progress:
            completed_stations: int = 0
            for station, (_, _, archive_file_paths) in stations.items():
                _LOGGER.debug("Loading corrections for station %s to %d files", station, len(archive_file_paths))
                progress.set_title(f"Applying {station.upper()} corrections")
                progress(completed_stations / len(stations))

                runner = station_data(station, 'editing', 'run')
                for archive, file_paths in archive_file_paths.items():
                    _LOGGER.debug("Applying %s/%s corrections to %d files", station, archive, len(file_paths))
                    available = RunAvailable(file_paths, self.data_path)
                    runner(available)

                completed_stations += 1

        correct_end_time = time.monotonic()
        _LOGGER.debug("Corrections applied in %.3f seconds", correct_end_time - correct_begin_time)

    async def __call__(self) -> None:
        self.ensure_writable()

        scan_begin_time = time.monotonic()
        stations: typing.Dict[str, typing.Tuple[int, int, typing.Dict[str, typing.List[Path]]]] = dict()
        for file in self.data_files():
            data_file = Dataset(str(file), 'r+')
            try:
                if self.override_station:
                    station_name = self.override_station
                else:
                    station_name = data_file.variables.get("station_name")
                    if station_name is None:
                        continue
                    station_name = str(station_name[0]).lower()

                try:
                    archive_mux = str(data_file.forge_archive).lower()
                except AttributeError:
                    archive_mux = ""

                start_time = getattr(data_file, 'time_coverage_start', None)
                if start_time is not None:
                    start_time = int(floor(parse_iso8601_time(str(start_time)).timestamp() * 1000))
                if not start_time:
                    start_time = 1

                end_time = getattr(data_file, 'time_coverage_end', None)
                if end_time is not None:
                    end_time = int(ceil(parse_iso8601_time(str(end_time)).timestamp() * 1000))
                if not end_time:
                    end_time = int(floor(time.time() * 1000))

                station_data = stations.get(station_name)
                if not station_data:
                    stations[station_name] = (start_time, end_time, {archive_mux: [file]})
                else:
                    archive_data = station_data[2].get(archive_mux)
                    if archive_data is None:
                        station_data[2][archive_mux] = [file]
                    else:
                        archive_data.append(file)
                    stations[station_name] = (min(start_time, station_data[0]), max(end_time, station_data[1]), station_data[2])
            finally:
                data_file.close()
        scan_end_time = time.monotonic()
        _LOGGER.debug("Edit file scan completed in %.3f seconds, with %d stations", scan_end_time - scan_begin_time, len(stations))

        if not self.apply_edits:
            await self._run_corrections(stations)
        elif not self.apply_corrections:
            await self._run_edits(stations)
        else:
            await self._run_edits(stations)
            await self._run_corrections(stations)
