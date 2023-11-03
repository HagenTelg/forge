import typing
import asyncio
import logging
import time
import shutil
from math import floor, ceil
from tempfile import TemporaryDirectory
from pathlib import Path
from netCDF4 import Dataset
from forge.logicaltime import round_to_year
from forge.archive.client.connection import Connection
from forge.archive.client.put import ArchivePut
from forge.archive.client.get import get_all_daily_files, get_all_yearly_files
from forge.data.structure.history import append_history

_LOGGER = logging.getLogger(__name__)


async def _run_avgh(connection: Connection, input_directory: Path, output_directory: Path,
                    station: str, start: int, end: int) -> int:
    for f in input_directory.iterdir():
        if not f.is_file():
            continue
        # connection.set_transaction_status()
        await asyncio.get_event_loop().run_in_executor(None, shutil.move,
                                                       str(f), str(output_directory / f.name))

    now = time.time()
    file_count = 0
    for file in output_directory.iterdir():
        if not file.name.endswith('.nc'):
            continue
        data = Dataset(str(file), 'r+')
        try:
            append_history(data, "forge.average.hourly", now)
        finally:
            data.close()
        file_count += 1
    return file_count


async def _write_avgh(connection: Connection, station: str, start: int, end: int,
                      source: Path, expected_count: int) -> None:
    put = ArchivePut(connection)
    await put.preemptive_lock_range(station, "avgh", start * 1000, end * 1000)

    total_written = 0
    expected_count = max(expected_count, 1)
    for file in source.iterdir():
        if not file.name.endswith('.nc'):
            continue
        if not file.is_file():
            continue
        await connection.set_transaction_status(f"Writing hourly averaged data, {total_written / expected_count:.0f}% done")

        data = Dataset(str(file), 'r')
        try:
            await put.data(data, archive="avgh", station=station, replace_entire=True)
        finally:
            data.close()

        total_written += 1

    await put.commit_index()


async def update_avgh_data(connection: Connection, station: str, start: float, end: float) -> None:
    start = int(floor(start / (24 * 60 * 60))) * 24 * 60 * 60
    end = int(ceil(end / (24 * 60 * 60))) * 24 * 60 * 60
    with TemporaryDirectory() as working_directory:
        working_directory = Path(working_directory)
        input_directory = working_directory / "input"
        input_directory.mkdir(exist_ok=True)
        _LOGGER.debug(f"Fetching clean data for {station.upper()} {start},{end} into {input_directory}")
        await connection.set_transaction_status("Loading clean data for averaging")
        await get_all_daily_files(connection, station, "clean", start, end, input_directory)

        output_directory = working_directory / "output"
        output_directory.mkdir(exist_ok=True)
        _LOGGER.debug(f"Running hourly averaging for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Starting hourly average calculation")
        total_files = await _run_avgh(connection, input_directory, output_directory, station, start, end)

        _LOGGER.debug(f"Writing {total_files} hourly averaged data files for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Writing hourly averaged data")
        await _write_avgh(connection, station, start, end, output_directory, total_files)


async def _run_avgd(connection: Connection, input_directory: Path, output_directory: Path,
                    station: str, start: int, end: int) -> int:
    # connection.set_transaction_status()

    now = time.time()
    file_count = 0
    for file in output_directory.iterdir():
        if not file.name.endswith('.nc'):
            continue
        data = Dataset(str(file), 'r+')
        try:
            append_history(data, "forge.average.daily", now)
        finally:
            data.close()
        file_count += 1
    return file_count


async def _write_avgd(connection: Connection, station: str, start: int, end: int,
                      source: Path, expected_count: int) -> None:
    put = ArchivePut(connection)
    await put.preemptive_lock_range(station, "avgd", start * 1000, end * 1000)

    total_written = 0
    expected_count = max(expected_count, 1)
    for file in source.iterdir():
        if not file.name.endswith('.nc'):
            continue
        if not file.is_file():
            continue
        await connection.set_transaction_status(f"Writing daily averaged data, {total_written / expected_count:.0f}% done")

        data = Dataset(str(file), 'r')
        try:
            await put.data(data, archive="avgd", station=station)
        finally:
            data.close()

        total_written += 1

    await put.commit_index()


async def update_avgd_data(connection: Connection, station: str, start: float, end: float) -> None:
    start = int(floor(start / (24 * 60 * 60))) * 24 * 60 * 60
    end = int(ceil(end / (24 * 60 * 60))) * 24 * 60 * 60
    with TemporaryDirectory() as working_directory:
        working_directory = Path(working_directory)
        input_directory = working_directory / "input"
        input_directory.mkdir(exist_ok=True)
        _LOGGER.debug(f"Fetching avgh data for {station.upper()} {start},{end} into {input_directory}")
        await connection.set_transaction_status("Loading hourly data for averaging")
        await get_all_daily_files(connection, station, "avgh", start, end, input_directory)

        output_directory = working_directory / "output"
        output_directory.mkdir(exist_ok=True)
        _LOGGER.debug(f"Running daily averaging for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Starting daily average calculation")
        total_files = await _run_avgd(connection, input_directory, output_directory, station, start, end)

        _LOGGER.debug(f"Writing {total_files} daily averaged data files for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Writing daily averaged data")
        await _write_avgd(connection, station, start, end, output_directory, total_files)


async def _run_avgm(connection: Connection, input_directory: Path, output_directory: Path,
                    station: str, start: int, end: int) -> int:
    # connection.set_transaction_status()

    now = time.time()
    file_count = 0
    for file in output_directory.iterdir():
        if not file.name.endswith('.nc'):
            continue
        data = Dataset(str(file), 'r+')
        try:
            append_history(data, "forge.average.monthly", now)
        finally:
            data.close()
        file_count += 1
    return file_count


async def _write_avgm(connection: Connection, station: str, start: int, end: int,
                      source: Path, expected_count: int) -> None:
    put = ArchivePut(connection)
    await put.preemptive_lock_range(station, "avgm", start * 1000, end * 1000)

    total_written = 0
    expected_count = max(expected_count, 1)
    for file in source.iterdir():
        if not file.name.endswith('.nc'):
            continue
        if not file.is_file():
            continue
        await connection.set_transaction_status(f"Writing monthly averaged data, {total_written / expected_count:.0f}% done")

        data = Dataset(str(file), 'r')
        try:
            await put.data(data, archive="avgm", station=station, replace_entire=True)
        finally:
            data.close()

        total_written += 1

    await put.commit_index()


async def update_avgm_data(connection: Connection, station: str, start: float, end: float) -> None:
    start, end = round_to_year(start, end)
    with TemporaryDirectory() as working_directory:
        working_directory = Path(working_directory)
        input_directory = working_directory / "input"
        input_directory.mkdir(exist_ok=True)
        _LOGGER.debug(f"Fetching avgd data for {station.upper()} {start},{end} into {input_directory}")
        await connection.set_transaction_status("Loading daily data for averaging")
        await get_all_yearly_files(connection, station, "avgd", start, end, input_directory)

        output_directory = working_directory / "output"
        output_directory.mkdir(exist_ok=True)
        _LOGGER.debug(f"Running monthly averaging for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Starting monthly average calculation")
        total_files = await _run_avgm(connection, input_directory, output_directory, station, start, end)

        _LOGGER.debug(f"Writing {total_files} monthly averaged data files for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Writing monthly averaged data")
        await _write_avgm(connection, station, start, end, output_directory, total_files)
