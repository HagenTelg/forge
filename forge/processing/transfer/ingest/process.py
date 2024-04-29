import typing
import asyncio
import logging
import time
import random
import shutil
import re
import os
from pathlib import Path
from netCDF4 import Dataset
from forge.crypto import PublicKey, key_to_bytes
from forge.archive.client.connection import Connection, LockDenied, LockBackoff
from forge.archive.client.put import ArchivePut, InvalidFile
from forge.processing.transfer import CONFIGURATION
from forge.processing.transfer.completion import completion_directory, FileType
from .file import file_complete, file_corrupted, file_error

_LOGGER = logging.getLogger(__name__)
_time_replace = re.compile(r"\{time:([^{}]+)}")


def _apply_completion(
        name: Path,
        destination: typing.Optional[typing.Union[bool, str]],
        station: str,
        key: typing.Optional[PublicKey] = None
) -> typing.Tuple[typing.Awaitable[Path], typing.Awaitable]:
    async def noop_before():
        return name

    async def noop_after():
        pass

    if destination is None:
        return noop_before(), noop_after()

    if not destination:
        async def do_remove():
            _LOGGER.debug("Removing %s", name)
            try:
                os.unlink(name)
            except OSError:
                _LOGGER.warning("Failed to remove file %s", name, exc_info=True)
            return None

        return noop_before(), do_remove()

    async def move_to_destination():
        nonlocal destination
        destination = completion_directory(destination, key, station, FileType.DATA)
        destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        destination = destination / name.name

        _LOGGER.debug("Moving %s to %s", name, destination)
        try:
            await asyncio.get_event_loop().run_in_executor(None, shutil.move,
                                                           str(name), str(destination))
            return destination
        except OSError:
            _LOGGER.warning("Failed to move file %s", name, exc_info=True)
            return name

    return move_to_destination(), noop_after()


class File:
    def __init__(self, name: Path, station: str, key: typing.Optional[PublicKey], preprocessing_time: float):
        self.name = name
        self.station = station
        if key:
            self._key_data = key_to_bytes(key)
        else:
            self._key_data = None
        self.total_time = preprocessing_time

    @property
    def key(self) -> typing.Optional[PublicKey]:
        if not self._key_data:
            return None
        return PublicKey.from_public_bytes(self._key_data)

    async def complete_corrupted(self) -> None:
        begin_before = time.monotonic()
        before, after = _apply_completion(
            self.name,
            CONFIGURATION.get('PROCESSING.TRANSFER.DATA.CORRUPT'),
            self.station,
            self.key,
        )
        self.name = await before
        self.total_time += time.monotonic() - begin_before
        await file_corrupted(self.name, self.station, self.total_time)
        await after

    async def complete_error(self) -> None:
        begin_before = time.monotonic()
        before, after = _apply_completion(
            self.name,
            CONFIGURATION.get('PROCESSING.TRANSFER.DATA.ERROR'),
            self.station,
            self.key,
        )
        self.name = await before
        self.total_time += time.monotonic() - begin_before
        await file_error(self.name, self.station, self.total_time)
        await after

    async def complete_ok(self) -> None:
        begin_before = time.monotonic()
        before, after = _apply_completion(
            self.name,
            CONFIGURATION.get('PROCESSING.TRANSFER.DATA.COMPLETE'),
            self.station,
            self.key,
        )
        self.name = await before

        try:
            file_data = Dataset(str(self.name), 'r')
        except:
            _LOGGER.debug("Error in completion open for file %s", self.name, exc_info=True)
            self.total_time += time.monotonic() - begin_before
            await self.complete_error()
            return
        try:
            self.total_time += time.monotonic() - begin_before
            await file_complete(self.name, file_data, self.station, self.total_time)
        finally:
            file_data.close()

        await after


def process_files(files: typing.List[File], station: str) -> None:
    try:
        loop = asyncio.get_running_loop()
    except AttributeError:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()

    _LOGGER.debug("Processing %d files for %s", len(files), station.upper())

    async def run():
        connection = await Connection.default_connection("data ingest")

        await connection.startup()

        files_first: typing.List[File] = files
        files_next: typing.List[File] = list()
        files_corrupted: typing.List[File] = list()
        files_error: typing.List[File] = list()

        begin_processing = time.monotonic()

        backoff = LockBackoff()
        try:
            while True:
                if not files_first:
                    if not files_next:
                        break
                    if backoff.failure_count > 100:
                        # Try one at a time after excessive failures
                        idx = random.randrange(len(files_next))
                        files_first.append(files_next[idx])
                        del files_next[idx]
                    else:
                        files_first.extend(files_next)
                        files_next.clear()

                files_accepted: typing.List[File] = list()
                try:
                    async with connection.transaction(True):
                        put = ArchivePut(connection)
                        for file in files_first:
                            try:
                                file_data = Dataset(str(file.name), 'r')
                            except FileNotFoundError:
                                _LOGGER.debug("File %s not found", file.name, exc_info=True)
                                file.total_time += time.monotonic() - begin_processing
                                files_error.append(file)
                                continue
                            except OSError:
                                _LOGGER.debug("Error in file %s", file.name, exc_info=True)
                                file.total_time += time.monotonic() - begin_processing
                                files_corrupted.append(file)
                                continue

                            try:
                                await put.auto(file_data, station=station)
                            except InvalidFile:
                                _LOGGER.debug("Invalid file %s", file.name, exc_info=True)
                                file.total_time += time.monotonic() - begin_processing
                                files_corrupted.append(file)
                                continue
                            except LockDenied:
                                # Put it in next to try later
                                files_next.append(file)
                                raise
                            finally:
                                file_data.close()
                                file_data = None

                            files_accepted.append(file)

                        await put.commit_index()

                        ingest_elapsed = time.monotonic() - begin_processing
                        for f in files_accepted:
                            f.total_time += ingest_elapsed
                            await f.complete_ok()
                        files_accepted.clear()
                        files_first.clear()
                        backoff.clear()
                except LockDenied as ld:
                    _LOGGER.debug("Archive busy: %s", ld.status)
                    await backoff()

                    # Lock denied files are already in next, so set first to those that didn't cause an immediate deny
                    files_first = files_accepted

            await connection.shutdown()
        finally:
            files_error.extend(files_first)
            files_error.extend(files_next)
            for file in files_corrupted:
                await file.complete_corrupted()
            for file in files_error:
                await file.complete_error()

    loop.run_until_complete(run())
