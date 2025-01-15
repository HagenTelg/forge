import typing
import asyncio
import logging
import time
import datetime
import os
import re
from math import floor, ceil
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from netCDF4 import Dataset
from forge.logicaltime import round_to_year
from forge.temp import WorkingDirectory
from forge.archive.client.connection import Connection
from forge.archive.client.put import ArchivePut
from forge.archive.client.get import get_all_daily_files, get_all_yearly_files
from forge.data.structure.history import append_history
from .run import process_avgh, process_avgd, process_avgm
from .merge import merge_files

_LOGGER = logging.getLogger(__name__)
_DAY_FILE_MATCH = re.compile(
    r'[A-Z][0-9A-Z_]{0,31}-([A-Z][A-Z0-9]*)_'
    r's((\d{4})(\d{2})(\d{2}))\.nc',
)


async def _write_files(connection: Connection, put: ArchivePut, source: Path, station: str, archive: str,
                       whole_file: bool, history: str, status: str) -> None:
    write_files: typing.List[Path] = list()
    for file in source.iterdir():
        if not file.name.endswith('.nc'):
            continue
        if not file.is_file():
            continue
        write_files.append(file)
        if len(write_files) % 256 == 0:
            await asyncio.sleep(0)

    history_time = time.time()
    for idx in range(len(write_files)):
        _LOGGER.debug("Writing average file %s/%s", station.upper(), write_files[idx].name)
        data = Dataset(str(write_files[idx]), 'r+')
        try:
            append_history(data, history, history_time)
            if whole_file:
                await put.replace_exact(data, archive=archive, station=station)
            else:
                await put.data(data, archive=archive, station=station)
        except:
            _LOGGER.error("Write error on file %s", write_files[idx].name)
            raise
        finally:
            if not whole_file:
                data.close()
            try:
                write_files[idx].unlink(missing_ok=True)
            except OSError:
                pass

        await connection.set_transaction_status(status.format(percent_done=((idx + 1) / len(write_files)) * 100.0))

    await put.commit_index()


async def _concurrent_run(connection: Connection, executor: ProcessPoolExecutor, station: str,
                          run: typing.Callable,
                          args: typing.List[typing.Tuple], status: str) -> None:
    concurrent_limit = max(os.cpu_count() + 2, 32)
    completed_calls: int = 0
    launched_calls: typing.Set[asyncio.Future] = set()

    async def process_launched():
        nonlocal launched_calls
        nonlocal completed_calls

        done, pending = await asyncio.wait(launched_calls, return_when=asyncio.FIRST_COMPLETED)
        launched_calls = set(pending)
        for check in done:
            check.result()
            completed_calls += 1
        await connection.set_transaction_status(status.format(percent_done=(completed_calls / len(args)) * 100.0))

    for a in args:
        launched = asyncio.get_event_loop().run_in_executor(executor, run, station, *a)

        launched_calls.add(launched)
        while len(launched_calls) > concurrent_limit:
            await process_launched()

    while launched_calls:
        await process_launched()


async def _run_avgh(connection: Connection, input_directory: Path, output_directory: Path,
                    station: str, start: int, end: int) -> None:
    with ProcessPoolExecutor() as executor:
        run_args: typing.List[typing.Tuple[str, str, str]] = list()
        for input_file in input_directory.iterdir():
            if not input_file.name.endswith('.nc'):
                continue
            if not input_file.is_file():
                continue
            run_args.append((str(input_file), str(output_directory / input_file.name), str(output_directory)))
            if len(run_args) % 256 == 0:
                await asyncio.sleep(0)

        await _concurrent_run(
            connection, executor, station, process_avgh, run_args,
            "Generating hourly averages, {percent_done:.0f}% done"
        )
        executor.shutdown(wait=True)


async def _write_avgh(connection: Connection, station: str, start: int, end: int,
                      source: Path) -> None:
    put = ArchivePut(connection)
    await put.preemptive_lock_range(station, "avgh", start * 1000, end * 1000)

    await _write_files(connection, put, source, station, "avgh", True, "forge.average.hourly",
                       "Writing hourly averaged data, {percent_done:.0f}% done")


async def update_avgh_data(connection: Connection, station: str, start: float, end: float) -> None:
    start = int(floor(start / (24 * 60 * 60))) * 24 * 60 * 60
    end = int(ceil(end / (24 * 60 * 60))) * 24 * 60 * 60
    async with WorkingDirectory() as working_directory:
        working_directory = Path(working_directory)
        input_directory = working_directory / "input"
        input_directory.mkdir(exist_ok=True)
        _LOGGER.debug(f"Fetching clean data for {station.upper()} {start},{end} into {input_directory}")
        await connection.set_transaction_status("Loading clean data for averaging")
        await get_all_daily_files(connection, station, "clean", start, end, input_directory,
                                  status_format="Loading clean data for averaging, {percent_done:.0f}% done")

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
    with ProcessPoolExecutor() as executor:
        run_args: typing.List[typing.Tuple[str, str]] = list()
        for input_file in input_directory.iterdir():
            if not input_file.name.endswith('.nc'):
                continue
            if not input_file.is_file():
                continue
            run_args.append((str(input_file), str(output_directory / input_file.name)))
            if len(run_args) % 256 == 0:
                await asyncio.sleep(0)

        await _concurrent_run(
            connection, executor, station, process_avgd, run_args,
            "Generating daily averages, {percent_done:.0f}% done"
        )
        executor.shutdown(wait=True)


async def _merge_avgd(connection: Connection, input_directory: Path, output_directory: Path,
                    station: str, start: int, end: int) -> None:
    with ProcessPoolExecutor() as executor:
        merge_sets: typing.Dict[typing.Tuple[str, int], typing.List[typing.Tuple[int, Path]]] = dict()
        total_file_count = 0
        for input_file in input_directory.iterdir():
            if not input_file.is_file():
                continue
            match = _DAY_FILE_MATCH.fullmatch(input_file.name)
            if not match:
                continue
            instrument_id = match.group(1)
            file_year = int(match.group(3))
            file_day_start = int(floor(datetime.datetime(
                int(match.group(3)), int(match.group(4)), int(match.group(5)),
                tzinfo=datetime.timezone.utc
            ).timestamp()))

            target = merge_sets.get((instrument_id, file_year))
            if not target:
                target = list()
                merge_sets[(instrument_id, file_year)] = target

            target.append((file_day_start, input_file))
            total_file_count += 1
            if total_file_count % 256 == 0:
                await asyncio.sleep(0)

        run_args: typing.List[typing.Tuple[typing.List[str], str]] = list()
        for input_files in merge_sets.values():
            input_files.sort(key=lambda x: x[0])
            for start_idx in range(0, len(input_files), 32):
                output_file = output_directory / input_files[start_idx][1].name
                run_args.append((
                    list([str(file) for _, file in input_files[start_idx:start_idx+32]]),
                    str(output_file)
                ))

        await _concurrent_run(
            connection, executor, station, merge_files, run_args,
            "Merging daily averages, {percent_done:.0f}% done"
        )
        executor.shutdown(wait=True)


async def _write_avgd(connection: Connection, station: str, start: int, end: int,
                      source: Path) -> None:
    put = ArchivePut(connection)
    start, end = round_to_year(start, end)
    await put.preemptive_lock_range(station, "avgd", start * 1000, end * 1000)

    await _write_files(connection, put, source, station, "avgd", False, "forge.average.daily",
                       "Writing daily averaged data, {percent_done:.0f}% done")


async def update_avgd_data(connection: Connection, station: str, start: float, end: float) -> None:
    start = int(floor(start / (24 * 60 * 60))) * 24 * 60 * 60
    end = int(ceil(end / (24 * 60 * 60))) * 24 * 60 * 60
    async with WorkingDirectory() as working_directory:
        working_directory = Path(working_directory)
        input_directory = working_directory / "input"
        input_directory.mkdir(exist_ok=True)
        _LOGGER.debug(f"Fetching avgh data for {station.upper()} {start},{end} into {input_directory}")
        await connection.set_transaction_status("Loading hourly data for averaging")
        await get_all_daily_files(connection, station, "avgh", start, end, input_directory,
                                  status_format="Loading hourly data for averaging, {percent_done:.0f}% done")

        average_directory = working_directory / "average"
        average_directory.mkdir(exist_ok=True)
        _LOGGER.debug(f"Running daily averaging for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Starting daily average calculation")
        await _run_avgd(connection, input_directory, average_directory, station, start, end)

        output_directory = working_directory / "output"
        output_directory.mkdir(exist_ok=True)
        _LOGGER.debug(f"Merging daily averages for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Starting daily average merging")
        await _merge_avgd(connection, average_directory, output_directory, station, start, end)

        _LOGGER.debug(f"Writing daily averaged data for {station.upper()} {start},{end}")
        await connection.set_transaction_status("Writing daily averaged data")
        await _write_avgd(connection, station, start, end, output_directory)
        _LOGGER.debug(f"Daily average write completed for {station.upper()} {start},{end}")


async def _run_avgm(connection: Connection, input_directory: Path, output_directory: Path,
                    station: str, start: int, end: int) -> None:
    with ProcessPoolExecutor() as executor:
        run_args: typing.List[typing.Tuple[str, str]] = list()
        for input_file in input_directory.iterdir():
            if not input_file.name.endswith('.nc'):
                continue
            if not input_file.is_file():
                continue
            run_args.append((str(input_file), str(output_directory / input_file.name)))
            if len(run_args) % 256 == 0:
                await asyncio.sleep(0)

        await _concurrent_run(
            connection, executor, station, process_avgm, run_args,
            "Generating monthly averages, {percent_done:.0f}% done"
        )
        executor.shutdown(wait=True)


async def _write_avgm(connection: Connection, station: str, start: int, end: int,
                      source: Path) -> None:
    put = ArchivePut(connection)
    await put.preemptive_lock_range(station, "avgm", start * 1000, end * 1000)

    await _write_files(connection, put, source, station, "avgm", True, "forge.average.monthly",
                       "Writing daily monthly data, {percent_done:.0f}% done")


async def update_avgm_data(connection: Connection, station: str, start: float, end: float) -> None:
    start, end = round_to_year(start, end)
    async with WorkingDirectory() as working_directory:
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
