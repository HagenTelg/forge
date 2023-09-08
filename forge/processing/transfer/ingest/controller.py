import typing
import asyncio
import logging
import time
import os
from collections import OrderedDict
from pathlib import Path
from netCDF4 import Dataset
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from forge.tasks import background_task, wait_cancelable
from forge.crypto import PublicKey
from forge.archive.client.put import ArchivePut, InvalidFile
from .process import File, process_files

_LOGGER = logging.getLogger(__name__)


class _StationQueue:
    def __init__(self, controller: "Controller", station: str):
        self.controller = controller
        self.station = station
        self._queued: typing.Dict[Path, File] = OrderedDict()
        self._processing: typing.Dict[Path, File] = OrderedDict()
        self._active_processing: typing.Optional[asyncio.Task] = None
        if controller.single_process:
            self._file_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f'Process{station.upper()}')
        else:
            self._file_executor = ProcessPoolExecutor(max_workers=1)

    async def _acquire_processing(self) -> bool:
        # Wait for queues to drop below system concurrency or for a timeout to elapse and start anyway
        begin_time = time.monotonic()
        concurrent_limit = max(os.cpu_count(), 2)

        # No context manager because we can abort a wait-acquire
        await self.controller.lock.acquire()
        try:
            self._processing.clear()
            if not self._queued:
                self.controller.lock.notify_all()
                return False

            while True:
                if self.controller.queues_processing(self) < concurrent_limit:
                    break
                elapsed_wait = time.monotonic() - begin_time
                sleep_remaining = 300.0 - elapsed_wait
                if sleep_remaining <= 0.0:
                    _LOGGER.warning("Timeout waiting for concurrency drop, stations(s) are probably stuck")
                    break
                try:
                    await wait_cancelable(self.controller.lock.wait(), sleep_remaining)
                except (TimeoutError, asyncio.TimeoutError):
                    # Acquire may have been aborted
                    if not self.controller.lock.locked():
                        await self.controller.lock.acquire()
                    break

            self._processing.update(self._queued)
            self._queued.clear()
            if not self._processing:
                self.controller.lock.notify_all()
                return False
            return True
        finally:
            self.controller.lock.release()

    async def _run(self) -> None:
        while True:
            if not await self._acquire_processing():
                break
            try:
                await asyncio.get_event_loop().run_in_executor(
                    self._file_executor, process_files,
                    list(self._processing.values()), self.station,
                )
            except:
                _LOGGER.error(f"Error processing files for {self.station}", exc_info=True)

        task = self._active_processing
        self._active_processing = None

        async def _reap():
            await self.controller.reap(self)
            try:
                await task
            except:
                pass

        background_task(_reap())

    def _wake(self) -> None:
        if self._active_processing:
            return
        self._active_processing = asyncio.get_event_loop().create_task(self._run())

    def add(self, file: File) -> None:
        if file.name in self._processing:
            _LOGGER.debug(f"Already processing file {file.name} on {self.station.upper()}")
            return
        if file.name in self._queued:
            _LOGGER.debug(f"Already queued file {file.name} on {self.station.upper()}")
            return
        _LOGGER.debug(f"Queued file {file.name} on {self.station.upper()}")
        self._queued[file.name] = file
        self._wake()

    def shutdown(self) -> None:
        self._file_executor.shutdown(wait=True)

    @property
    def is_idle(self) -> bool:
        return not self._queued and not self._processing

    @property
    def is_processing(self) -> bool:
        return len(self._processing) != 0


class Controller:
    def __init__(self, single_process: bool = False):
        self.lock = asyncio.Condition()
        self._queues: typing.Dict[str, _StationQueue] = dict()
        self.single_process = single_process

    def shutdown(self) -> None:
        for q in self._queues.values():
            q.shutdown()
        self._queues.clear()

    def queues_processing(self, exclude: _StationQueue) -> int:
        count = 0
        for q in self._queues.values():
            if q == exclude:
                continue
            if not q.is_processing:
                continue
            count += 1
        return count

    async def reap(self, station_queue: _StationQueue) -> None:
        async with self.lock:
            existing = self._queues.get(station_queue.station)
            if not existing:
                return
            if existing != station_queue:
                return
            if not existing.is_idle:
                return
            del self._queues[station_queue.station]
            self.lock.notify_all()

    async def enqueue(self, file: Path, station: str = None, key: typing.Optional[PublicKey] = None) -> bool:
        try:
            file = file.resolve(strict=True)
        except (FileNotFoundError, RuntimeError):
            _LOGGER.debug(f"Error resolving file {file}", exc_info=True)
            return False
        if not file.exists() or not file.is_file():
            _LOGGER.debug(f"File {file} does not exist")
            return False

        begin_processing = time.monotonic()
        try:
            file_data = Dataset(str(file), 'r')
        except FileNotFoundError:
            _LOGGER.debug(f"File {file} does not exist", exc_info=True)
            return False
        except OSError:
            _LOGGER.debug(f"Error in initial open check for {file}", exc_info=True)
            if station:
                await File(file, station, key, time.monotonic() - begin_processing).complete_corrupted()
                # File "processed" as corrupted, so tell the client ok
                return True
            else:
                return False
        if not station:
            try:
                station = ArchivePut.get_station(file_data)
            except InvalidFile:
                _LOGGER.debug(f"Failed to determine station for file {file}", exc_info=True)
                return False

        file_data.close()
        preprocessing_time = time.monotonic() - begin_processing

        station = station.lower()
        async with self.lock:
            target = self._queues.get(station)
            if not target:
                target = _StationQueue(self, station)
                self._queues[station] = target
            target.add(File(file, station, key, preprocessing_time))

        return True
