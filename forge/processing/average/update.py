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


async def _write_files(connection: Connection, put: ArchivePut, source: Path, station: str, archive: str,
                       whole_file: bool, history: str, status: str) -> None:
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
        try:
            append_history(data, history, history_time)
            if whole_file:
                await put.replace_exact(data, archive=archive, station=station)
            else:
                await put.data(data, archive=archive, station=station)
        finally:
            if not whole_file:
                data.close()

        await connection.set_transaction_status(status.format(percent_done=((idx + 1) / len(write_files)) * 100.0))

    await put.commit_index()


async def _run_avgh(connection: Connection, input_directory: Path, output_directory: Path,
                    station: str, start: int, end: int) -> None:
    for f in input_directory.iterdir():
        if not f.is_file():
            continue
        # connection.set_transaction_status()
        await asyncio.get_event_loop().run_in_executor(None, shutil.move,
                                                       str(f), str(output_directory / f.name))


async def _write_avgh(connection: Connection, station: str, start: int, end: int,
                      source: Path) -> None:
    put = ArchivePut(connection)
    await put.preemptive_lock_range(station, "avgh", start * 1000, end * 1000)

    await _write_files(connection, put, source, station, "avgh", True, "forge.average.hourly",
                       "Writing hourly averaged data, {percent_done:.0f}% done")


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
        await _run_avgh(connection, input_directory, output_directory, station, start, end)

        _LOGGER.debug(f"Writing hourly averaged data for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Writing hourly averaged data")
        await _write_avgh(connection, station, start, end, output_directory)
        _LOGGER.debug(f"Hourly average write completed for {station.upper()} {start},{end}")


async def _run_avgd(connection: Connection, input_directory: Path, output_directory: Path,
                    station: str, start: int, end: int) -> None:
    # connection.set_transaction_status()
    pass


async def _write_avgd(connection: Connection, station: str, start: int, end: int,
                      source: Path) -> None:
    put = ArchivePut(connection)
    await put.preemptive_lock_range(station, "avgd", start * 1000, end * 1000)

    await _write_files(connection, put, source, station, "avgd", False, "forge.average.daily",
                       "Writing daily averaged data, {percent_done:.0f}% done")


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
        await _run_avgd(connection, input_directory, output_directory, station, start, end)

        _LOGGER.debug(f"Writing daily averaged data for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Writing daily averaged data")
        await _write_avgd(connection, station, start, end, output_directory)
        _LOGGER.debug(f"Daily average write completed for {station.upper()} {start},{end}")


async def _run_avgm(connection: Connection, input_directory: Path, output_directory: Path,
                    station: str, start: int, end: int) -> None:
    # connection.set_transaction_status()
    pass


async def _write_avgm(connection: Connection, station: str, start: int, end: int,
                      source: Path) -> None:
    put = ArchivePut(connection)
    await put.preemptive_lock_range(station, "avgm", start * 1000, end * 1000)

    await _write_files(connection, put, source, station, "avgm", False, "forge.average.monthly",
                       "Writing daily monthly data, {percent_done:.0f}% done")


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
        await _run_avgm(connection, input_directory, output_directory, station, start, end)

        _LOGGER.debug(f"Writing monthly averaged data for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Writing monthly averaged data")
        await _write_avgm(connection, station, start, end, output_directory)
        _LOGGER.debug(f"Monthly average write completed for {station.upper()} {start},{end}")
