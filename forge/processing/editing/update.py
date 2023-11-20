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
from concurrent.futures import ProcessPoolExecutor
from netCDF4 import Dataset
from forge.logicaltime import containing_year_range, start_of_year
from forge.archive.client.connection import Connection
from forge.archive.client.put import ArchivePut
from forge.archive.client.get import read_file_or_nothing, get_all_daily_files
from forge.archive.client import edit_directives_lock_key, edit_directives_file_name
from forge.data.structure.history import append_history
from .run import process_day

_LOGGER = logging.getLogger(__name__)
_DAY_FILE_MATCH = re.compile(
    r'[A-Z][0-9A-Z_]{0,31}-[A-Z][A-Z0-9]*_'
    r's((\d{4})(\d{2})(\d{2}))\.nc',
)


async def _fetch_edits(connection: Connection, station: str, start: int, end: int, destination: Path) -> None:
    await connection.lock_read(edit_directives_lock_key(station), start * 1000, end * 1000)

    await read_file_or_nothing(connection, edit_directives_file_name(station, None), destination)
    for year in range(*containing_year_range(start, end)):
        year_start = start_of_year(year)
        await read_file_or_nothing(connection, edit_directives_file_name(station, year_start), destination)


async def _run_editing(connection: Connection, working_directory: Path, station: str, start: int, end: int) -> None:
    with ProcessPoolExecutor() as executor:
        index_offset = int(floor(start / (24 * 60 * 60)))
        total_days: int = 0
        run_args: typing.List[typing.Optional[typing.Tuple[int, typing.List[str], typing.List[str]]]] = list()
        for file in (working_directory / "data").iterdir():
            if not file.is_file():
                continue
            match = _DAY_FILE_MATCH.fullmatch(file.name)
            if not match:
                continue
            file_day_start = int(floor(datetime.datetime(
                int(match.group(2)), int(match.group(3)), int(match.group(4)),
                tzinfo=datetime.timezone.utc
            ).timestamp()))
            target_index = int(floor(file_day_start / (24 * 60 * 60))) - index_offset
            while target_index >= len(run_args):
                run_args.append(None)

            if run_args[target_index] is None:
                total_days += 1
                year_start = start_of_year(int(match.group(2)))

                edit_files: typing.List[str] = list()
                add_edit_file = working_directory / "edits" / edit_directives_file_name(station, None)
                if add_edit_file.exists():
                    edit_files.append(str(add_edit_file))
                add_edit_file = working_directory / "edits" / edit_directives_file_name(station, year_start)
                if add_edit_file.exists():
                    edit_files.append(str(add_edit_file))

                run_args[target_index] = (file_day_start, edit_files, list())

            run_args[target_index][2].append(str(file))

        concurrent_limit = max(os.cpu_count()+2, 32)
        completed_days: int = 0
        launched_days: typing.Set[asyncio.Future] = set()
        output_directory = str(working_directory / "data")

        async def process_launched():
            nonlocal launched_days
            nonlocal completed_days

            done, pending = await asyncio.wait(launched_days, return_when=asyncio.FIRST_COMPLETED)
            launched_days = set(pending)
            for check in done:
                check.result()
                completed_days += 1
            await connection.set_transaction_status(f"Editing data, {(completed_days / total_days) * 100.0:.0f}% done")

        for day_args in run_args:
            launched = asyncio.get_event_loop().run_in_executor(
                executor, process_day,
                station, output_directory, *day_args,
            )

            launched_days.add(launched)
            while len(launched_days) > concurrent_limit:
                await process_launched()

        while launched_days:
            await process_launched()
        executor.shutdown(wait=True)


async def _write_data(connection: Connection, station: str, start: int, end: int, source: Path) -> None:
    put = ArchivePut(connection)
    await put.preemptive_lock_range(station, "edited", start * 1000, end * 1000)

    write_files: typing.List[Path] = list()
    for file in source.iterdir():
        if not file.name.endswith('.nc'):
            continue
        if not file.is_file():
            continue
        write_files.append(file)

    history_time = time.time()
    for idx in range(len(write_files)):
        data = Dataset(str(write_files[idx]), 'r+')
        append_history(data, "forge.editing", history_time)
        await put.data(data, archive="edited", station=station, exact_contents=True)

        percent_done = ((idx + 1) / len(write_files)) * 100.0
        await connection.set_transaction_status(f"Writing edited data, {percent_done:.0f}% done")

    await put.commit_index()


async def update_edited_data(connection: Connection, station: str, start: float, end: float) -> None:
    start = int(floor(start / (24 * 60 * 60))) * 24 * 60 * 60
    end = int(ceil(end / (24 * 60 * 60))) * 24 * 60 * 60
    begin_time = time.monotonic()
    with TemporaryDirectory() as working_directory:
        working_directory = Path(working_directory)
        data_directory = working_directory / "data"
        data_directory.mkdir(exist_ok=True)
        _LOGGER.debug(f"Fetching raw data for {station.upper()} {start},{end} into {data_directory}")
        await connection.set_transaction_status("Loading raw data for editing")
        await get_all_daily_files(connection, station, "raw", start, end, data_directory)

        edits_directory = working_directory / "edits"
        edits_directory.mkdir(exist_ok=True)
        _LOGGER.debug(f"Fetching edits for {station.upper()} {start},{end} into {edits_directory}")
        await connection.set_transaction_status("Loading edit directives")
        await _fetch_edits(connection, station, start, end, edits_directory)

        _LOGGER.debug(f"Running editing for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Starting editing")
        await _run_editing(connection, working_directory, station, start, end)

        _LOGGER.debug(f"Writing edited data for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Writing edited data")
        await _write_data(connection, station, start, end, data_directory)
        _LOGGER.debug(f"Edited write completed for {station.upper()} {start},{end}")
    _LOGGER.debug(f"Edited data update for {station.upper()} {start},{end} completed in {time.monotonic() - begin_time:.3f} seconds")
