import typing
import asyncio
import logging
import time
import datetime
import os
import re
from math import floor, ceil
from tempfile import mkstemp
from concurrent.futures import ProcessPoolExecutor
from forge.temp import WorkingDirectory
from forge.logicaltime import containing_year_range, year_bounds_ms
from forge.archive.client import index_lock_key, index_file_name, data_lock_key, data_file_name
from forge.archive.client.connection import Connection, LockDenied, LockBackoff
from forge.archive.client.archiveindex import ArchiveIndex
from forge.product.integrity.calculate import calculate_integrity


_LOGGER = logging.getLogger(__name__)
_ARCHIVE_FILE_MATCH = re.compile(
    r'[A-Z][0-9A-Z_]{0,31}-([A-Z][A-Z0-9]*)_'
    r's(\d{4})(\d{2})(\d{2})\.nc',
)
_HASH_TIME_REPLACE = re.compile(r"\{integrity:([^{}]+)}", flags=re.IGNORECASE)
_MODIFIED_TIME_REPLACE = re.compile(r"\{modified:([^{}]+)}", flags=re.IGNORECASE)


async def calculate_archive_integrity(
        connection: Connection, station: str, archive: str,
        instrument_limit: typing.Set[str],
        start_epoch_ms: int, end_epoch_ms: int,
) -> typing.AsyncIterator[typing.Tuple[str, float, float, bytes, bytes, bytes]]:
    with ProcessPoolExecutor() as executor:
        concurrent_limit = max(os.cpu_count() + 2, 32)
        launched_files: typing.Set[asyncio.Future] = set()
        launched_info: typing.Dict[asyncio.Future, typing.Tuple[str, str, float]] = dict()

        async with WorkingDirectory() as working_directory:
            transaction_begin: float = 0

            async def launch_file(file_time_ms: int, instrument_id: str) -> None:
                nonlocal launched_files
                nonlocal launched_info

                archive_file_name = data_file_name(station, archive, instrument_id, file_time_ms / 1000.0)

                fd, local_name = mkstemp(suffix='.nc', dir=working_directory)
                os.close(fd)
                try:
                    with open(local_name, 'wb') as dest:
                        await connection.read_file(archive_file_name, dest)
                except FileNotFoundError:
                    await asyncio.get_event_loop().run_in_executor(None, os.unlink, local_name)
                    return

                _LOGGER.debug(f"Starting integrity calculation on {archive_file_name}")
                launched = asyncio.get_event_loop().run_in_executor(executor, calculate_integrity,local_name)
                launched_files.add(launched)
                launched_info[launched] = (local_name, archive_file_name, transaction_begin)

            async def process_launched() -> typing.AsyncIterator[typing.Tuple[str, float, float, bytes, bytes, bytes]]:
                nonlocal launched_files
                nonlocal launched_info

                done, pending = await asyncio.wait(launched_files, return_when=asyncio.FIRST_COMPLETED)
                launched_files = set(pending)
                for check in done:
                    local_name, archive_file_name, hash_time = launched_info.pop(check)
                    try:
                        file_creation_time, file_hash, data_hash, qualitative_hash = check.result()
                    except:
                        _LOGGER.error(f"Error processing {archive_file_name}")
                        raise

                    _LOGGER.debug(f"Integrity calculation completed for {archive_file_name}")
                    await asyncio.get_event_loop().run_in_executor(None, os.unlink, local_name)
                    yield archive_file_name, file_creation_time, hash_time, file_hash, data_hash, qualitative_hash

            for year in range(*containing_year_range(start_epoch_ms / 1000.0, end_epoch_ms / 1000.0)):
                year_start_ms, year_end_ms = year_bounds_ms(year)

                if archive in ("avgd", "avgm"):
                    inspect_start_ms = year_start_ms
                    inspect_end_ms = year_end_ms
                else:
                    inspect_start_ms = max(start_epoch_ms, year_start_ms)
                    inspect_end_ms = min(year_end_ms, end_epoch_ms)
                    inspect_start_ms = int(floor(inspect_start_ms / (24 * 60 * 60 * 1000))) * 24 * 60 * 60 * 1000
                    inspect_end_ms = int(ceil(inspect_end_ms / (24 * 60 * 60 * 1000))) * 24 * 60 * 60 * 1000

                backoff = LockBackoff()
                while True:
                    try:
                        async with connection.transaction():
                            await connection.lock_read(index_lock_key(station, archive), inspect_start_ms, inspect_end_ms)
                            try:
                                index_contents = await connection.read_bytes(
                                    index_file_name(station, archive, year_start_ms / 1000.0)
                                )
                            except FileNotFoundError:
                                _LOGGER.debug(f"No index present for {station.upper()}/{archive.upper()}/{year}")
                                break
                            archive_index = ArchiveIndex(index_contents)

                            instrument_ids = set(archive_index.known_instrument_ids)
                            if instrument_limit:
                                instrument_ids = instrument_ids.intersection(instrument_limit)
                            if not instrument_ids:
                                _LOGGER.debug(f"No instruments present for {station.upper()}/{archive.upper()}/{year}")
                                break

                            await connection.lock_read(data_lock_key(station, archive), inspect_start_ms, inspect_end_ms)
                            transaction_begin = time.time()
                            if archive in ("avgd", "avgm"):
                                completed_instruments = 0
                                for instrument in instrument_ids:
                                    await launch_file(year_start_ms, instrument)

                                    while len(launched_files) > concurrent_limit:
                                        async for result in process_launched():
                                            yield result

                                    await connection.set_transaction_status(f"Calculating data integrity, {completed_instruments / (len(instrument_ids)-1) * 100:.0f}% done")
                                    completed_instruments += 1
                            else:
                                for day_start_ms in range(inspect_start_ms, inspect_end_ms, 24 * 60 * 60 * 1000):
                                    for instrument in instrument_ids:
                                        await launch_file(day_start_ms, instrument)

                                        while len(launched_files) > concurrent_limit:
                                            async for result in process_launched():
                                                yield result

                                    await connection.set_transaction_status(f"Calculating data integrity, {(day_start_ms - inspect_start_ms) / (inspect_end_ms - inspect_start_ms) * 100:.0f}% done")
                        break
                    except LockDenied as ld:
                        _LOGGER.debug("Archive busy: %s", ld.status)
                        await backoff()

            while launched_files:
                async for result in process_launched():
                    yield result

        executor.shutdown(wait=True)


def apply_output_pattern(
        target_file: str,
        station: str, archive: str, archive_file_name: str,
        hash_time: float, file_creation_time: typing.Optional[float],
) -> str:
    target_file = target_file.replace('{station}', station.lower())
    target_file = target_file.replace('{STATION}', station.upper())
    target_file = target_file.replace('{archive}', archive.lower())
    target_file = target_file.replace('{ARCHIVE}', archive.upper())

    m = _ARCHIVE_FILE_MATCH.fullmatch(archive_file_name.split('/')[-1])
    if m:
        target_file = target_file.replace('{instrument}', m.group(1))
        target_file = target_file.replace('{INSTRUMENT}', m.group(1))
        target_file = target_file.replace('{year}', m.group(2))
        target_file = target_file.replace('{YEAR}', m.group(2))
        target_file = target_file.replace('{month}', m.group(3))
        target_file = target_file.replace('{MONTH}', m.group(3))
        target_file = target_file.replace('{day}', m.group(4))
        target_file = target_file.replace('{DAY}', m.group(4))
    else:
        target_file = target_file.replace('{instrument}', '')
        target_file = target_file.replace('{INSTRUMENT}', '')
        target_file = target_file.replace('{year}', '')
        target_file = target_file.replace('{YEAR}', '')
        target_file = target_file.replace('{month}', '')
        target_file = target_file.replace('{MONTH}', '')
        target_file = target_file.replace('{day}', '')
        target_file = target_file.replace('{DAY}', '')

    if '{integrity' in target_file:
        hash_time = datetime.datetime.fromtimestamp(hash_time, tz=datetime.timezone.utc)
        target_file = _HASH_TIME_REPLACE.sub(lambda m: hash_time.strftime(m.group(1)), target_file)

    if '{modified' in target_file:
        if file_creation_time:
            modified_time = datetime.datetime.fromtimestamp(hash_time, tz=datetime.timezone.utc)
            target_file = _MODIFIED_TIME_REPLACE.sub(lambda m: modified_time.strftime(m.group(1)), target_file)
        else:
            target_file = _MODIFIED_TIME_REPLACE.sub(lambda m: '', target_file)

    return target_file