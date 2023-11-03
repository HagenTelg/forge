import typing
import asyncio
import logging
import time
from math import floor, ceil
from tempfile import TemporaryDirectory
from pathlib import Path
from netCDF4 import Dataset
from forge.logicaltime import containing_year_range, start_of_year
from forge.archive.client.connection import Connection
from forge.archive.client.put import ArchivePut
from forge.archive.client.get import read_file_or_nothing, get_all_daily_files
from forge.archive.client import edit_directives_lock_key, edit_directives_file_name
from forge.data.structure.history import append_history

_LOGGER = logging.getLogger(__name__)


async def _fetch_edits(connection: Connection, station: str, start: int, end: int, destination: Path) -> None:
    await connection.lock_read(edit_directives_lock_key(station), start * 1000, end * 1000)

    await read_file_or_nothing(connection, edit_directives_file_name(station, None), destination)
    for year in range(*containing_year_range(start, end)):
        year_start = start_of_year(year)
        await read_file_or_nothing(connection, edit_directives_file_name(station, year_start), destination)


async def _run_editing(connection: Connection, working_directory: Path, station: str, start: int, end: int) -> int:
    # connection.set_transaction_status()

    now = time.time()
    file_count = 0
    for file in (working_directory / "data").iterdir():
        if not file.name.endswith('.nc'):
            continue
        data = Dataset(str(file), 'r+')
        try:
            append_history(data, "forge.editing", now)
        finally:
            data.close()
        file_count += 1
    return file_count


async def _write_data(connection: Connection, station: str, start: int, end: int,
                      source: Path, expected_count: int) -> None:
    put = ArchivePut(connection)
    await put.preemptive_lock_range(station, "edited", start * 1000, end * 1000)

    total_written = 0
    expected_count = max(expected_count, 1)
    for file in source.iterdir():
        if not file.name.endswith('.nc'):
            continue
        if not file.is_file():
            continue
        data = Dataset(str(file), 'r')
        try:
            await put.data(data, archive="edited", station=station, replace_entire=True)
        finally:
            data.close()

        total_written += 1
        await connection.set_transaction_status(f"Writing edited data, {total_written / expected_count:.0f}% done")

    await put.commit_index()


async def update_edited_data(connection: Connection, station: str, start: float, end: float) -> None:
    start = int(floor(start / (24 * 60 * 60))) * 24 * 60 * 60
    end = int(ceil(end / (24 * 60 * 60))) * 24 * 60 * 60
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
        total_files = await _run_editing(connection, working_directory, station, start, end)

        _LOGGER.debug(f"Writing {total_files} edited data files for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Writing edited data")
        await _write_data(connection, station, start, end, data_directory, total_files)
