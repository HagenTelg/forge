import typing
import asyncio
import logging
import time
import datetime
import re
import os
from math import floor, ceil
from tempfile import TemporaryDirectory
from pathlib import Path
from netCDF4 import Dataset
from forge.logicaltime import containing_year_range, start_of_year
from forge.formattime import format_iso8601_time
from forge.timeparse import parse_iso8601_time
from forge.archive.client.connection import Connection
from forge.archive.client.put import ArchivePut
from forge.archive.client.get import read_file_or_nothing, get_all_daily_files
from forge.archive.client import passed_lock_key, passed_file_name
from forge.data.structure.history import append_history
from .filter import AcceptIntoClean

_LOGGER = logging.getLogger(__name__)
_DAY_FILE_MATCH = re.compile(
    r'[A-Z][0-9A-Z_]{0,31}-[A-Z][A-Z0-9]*_'
    r's((\d{4})(\d{2})(\d{2}))\.nc',
)


async def _fetch_passed(connection: Connection, station: str, start: int, end: int, destination: Path) -> None:
    await connection.lock_read(passed_lock_key(station), start * 1000, end * 1000)

    for year in range(*containing_year_range(start, end)):
        year_start = start_of_year(year)
        await read_file_or_nothing(connection, passed_file_name(station, year_start), destination)


async def _run_pass(connection: Connection, working_directory: Path, station: str, start: int, end: int) -> None:
    index_offset = int(floor(start / (24 * 60 * 60)))
    total_count = int(ceil(end / (24 * 60 * 60))) - index_offset
    day_files: typing.List[typing.Set[Path]] = list()
    total_file_count = 0
    for file in (working_directory / "data").iterdir():
        if not file.is_file():
            continue
        match = _DAY_FILE_MATCH.fullmatch(file.name)
        if not match:
            file.unlink()
            continue
        file_day_number = int(floor(datetime.datetime(
            int(match.group(2)), int(match.group(3)), int(match.group(4)),
            tzinfo=datetime.timezone.utc
        ).timestamp() / (24 * 60 * 60)))
        target_index = file_day_number - index_offset
        if target_index < 0 or target_index >= total_count:
            continue
        while target_index >= len(day_files):
            day_files.append(set())
        day_files[target_index].add(file)
        total_file_count += 1
        if total_file_count % 256 == 0:
            await asyncio.sleep(0)

    _LOGGER.debug(f"Processing clean data {start},{end} split into {len(day_files)} days with {total_file_count} files")

    pass_time = time.time()
    total_file_count = 0
    current_year: typing.Optional[int] = None
    current_filter: typing.Optional[AcceptIntoClean] = None
    for day_index in range(len(day_files)):
        file_start = (day_index + index_offset) * (24 * 60 * 60)
        file_end = file_start + 24 * 60 * 60

        try:
            await connection.set_transaction_status(f"Processing clean data, {(day_index / total_count) * 100.0:.0f}% done")

            file_year = time.gmtime(file_start).tm_year
            if file_year != current_year:
                if current_filter:
                    current_filter.close()
                current_year = file_year
                passed_file = working_directory / f"passed/{station.upper()}-PASSED_s{current_year:04}0101.nc"
                if not passed_file.exists():
                    current_filter = None
                else:
                    year_start = start_of_year(current_year)
                    year_end = start_of_year(current_year + 1)
                    current_filter = AcceptIntoClean(
                        station, str(passed_file),
                        max(year_start, file_start),
                        min(year_end, end),
                    )

            if current_filter is None:
                for remove_file in day_files[day_index]:
                    remove_file.unlink()
                continue

            for check_file in day_files[day_index]:
                data = current_filter.accept_file(file_start, file_end, str(check_file))
                if data is None:
                    check_file.unlink()
                    continue
                data, profile_pass_time = data
                try:
                    append_history(data, "forge.pass", pass_time)
                    data.setncattr("data_pass_time", format_iso8601_time(profile_pass_time))
                finally:
                    data.close()
                total_file_count += 1
        except:
            _LOGGER.error(f"Error generating clean for %s day %d", station.upper(), file_start, exc_info=True)
            raise

    if current_filter:
        current_filter.close()


async def _write_data(connection: Connection, station: str, start: int, end: int, source: Path) -> None:
    put = ArchivePut(connection)
    await put.preemptive_lock_range(station, "clean", start * 1000, end * 1000)

    write_files: typing.List[Path] = list()
    for file in source.iterdir():
        if not file.name.endswith('.nc'):
            continue
        if not file.is_file():
            continue
        write_files.append(file)
        if len(write_files) % 256 == 0:
            await asyncio.sleep(0)

    for idx in range(len(write_files)):
        _LOGGER.debug("Writing clean file %s/%s", station.upper(), write_files[idx].name)

        def replace_file(original: typing.Optional[Dataset], replacement: Dataset) -> bool:
            if original is None:
                return True

            original_pass_time = getattr(original, 'data_pass_time', None)
            if original_pass_time is None:
                return True
            original_pass_time = parse_iso8601_time(str(original_pass_time)).timestamp()

            replace_pass_time = getattr(replacement, 'data_pass_time', None)
            if replace_pass_time is None:
                return True
            replace_pass_time = parse_iso8601_time(str(replace_pass_time)).timestamp()

            return original_pass_time < replace_pass_time

        data = Dataset(str(write_files[idx]), 'r+')
        await put.replace_exact(data, archive="clean", station=station, replace_existing=replace_file)

        try:
            write_files[idx].unlink()
        except (OSError, FileNotFoundError):
            pass

        percent_done = ((idx + 1) / len(write_files)) * 100.0
        await connection.set_transaction_status(f"Writing clean data, {percent_done:.0f}% done")

    await put.commit_index()


async def update_clean_data(connection: Connection, station: str, start: float, end: float) -> None:
    start = int(floor(start / (24 * 60 * 60))) * 24 * 60 * 60
    end = int(ceil(end / (24 * 60 * 60))) * 24 * 60 * 60
    with TemporaryDirectory() as working_directory:
        working_directory = Path(working_directory)
        data_directory = working_directory / "data"
        data_directory.mkdir(exist_ok=True)
        _LOGGER.debug(f"Fetching edited data for {station.upper()} {start},{end} into {data_directory}")
        await connection.set_transaction_status("Loading edited data for passing")
        await get_all_daily_files(connection, station, "edited", start, end, data_directory,
                                  status_format="Loading edited data for passing, {percent_done:.0f}% done")

        passed_directory = working_directory / "passed"
        passed_directory.mkdir(exist_ok=True)
        _LOGGER.debug(f"Fetching passed ranges for {station.upper()} {start},{end} into {passed_directory}")
        await connection.set_transaction_status("Loading passed ranges")
        await _fetch_passed(connection, station, start, end, passed_directory)

        _LOGGER.debug(f"Running pass filtering for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Processing clean data")
        await _run_pass(connection, working_directory, station, start, end)

        _LOGGER.debug(f"Writing clean data for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Writing clean data")
        await _write_data(connection, station, start, end, data_directory)
