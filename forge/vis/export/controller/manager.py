import typing
import asyncio
import time
import logging
import os
import sys
from math import floor
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from tempfile import NamedTemporaryFile
from zipfile import ZipFile, ZIP_DEFLATED
from pathlib import Path
from shutil import copyfileobj
from forge.tasks import background_task
from forge.temp import WorkingDirectory
from forge.vis import CONFIGURATION
from forge.vis.export import Export
from forge.vis.export.assemble import export_data


_LOGGER = logging.getLogger(__name__)
_THREAD_POOL = ThreadPoolExecutor(thread_name_prefix="Export")


class _ExportKey:
    def __init__(self, station: str, mode_name: str, export_key: str,
                       start_epoch_ms: int, end_epoch_ms: int):
        self.args = (station, mode_name, export_key, start_epoch_ms, end_epoch_ms)

    def __eq__(self, other):
        if not isinstance(other, _ExportKey):
            return NotImplemented
        return self.args == other.args

    def __hash__(self):
        return hash(self.args)

    def __repr__(self):
        return self.args.__repr__()


class ExportedFile:
    def __init__(self, file: NamedTemporaryFile, client_name: str = 'export.zip', media_type: str = 'application/zip'):
        self.file = file
        self.size = os.fstat(file.fileno()).st_size
        self.client_name = client_name
        self.media_type = media_type
        self.accessed = time.monotonic()


class _ExportRequest:
    _DIRECTORY = CONFIGURATION.get('EXPORT.DIRECTORY', '/var/tmp')

    def __init__(self, station: str, mode_name: str, export_key: str,
                 start_epoch_ms: int, end_epoch_ms: int):
        self.station = station
        self.mode_name = mode_name
        self.export_key = export_key
        self.start_epoch_ms = start_epoch_ms
        self.end_epoch_ms = end_epoch_ms
        self._attached: typing.List[asyncio.Future] = list()
        self._check_canceled: typing.Optional[typing.Callable[[], None]] = None

    def attach(self) -> asyncio.Future:
        result = asyncio.get_event_loop().create_future()
        self._attached.append(result)
        if self._check_canceled:
            result.add_done_callback(self._check_canceled)
        return result

    @staticmethod
    def _create_zip(directory: str, target_zip: str) -> None:
        with ZipFile(target_zip, mode='w', compression=ZIP_DEFLATED) as target:
            def walk_directory(root: Path, prefix: str) -> None:
                sub_dirs = []
                for file in root.iterdir():
                    if file.name.startswith("."):
                        continue
                    if file.is_dir():
                        sub_dirs.append(file)
                        continue
                    if not file.is_file():
                        continue
                    target.write(str(file), arcname=prefix + file.name)
                for dir in sub_dirs:
                    walk_directory(dir, prefix + dir.name + "/")

            walk_directory(Path(directory), "")


    def _export_zip_name(self):
        station = self.station.upper()
        mode = self.mode_name.upper().replace('-', '')
        key = self.export_key.upper().replace('-', '')
        ts = time.gmtime(floor(self.start_epoch_ms / 1000))
        return f"{station}_{mode}_{key}_{ts.tm_year:04}{ts.tm_mon:02}{ts.tm_mday:02}.zip"

    def _apply_result(self, result: typing.Optional[ExportedFile]) -> None:
        for r in self._attached:
            try:
                r.set_result(result)
            except (asyncio.CancelledError, asyncio.InvalidStateError):
                pass
        self._attached.clear()

    async def run(self) -> typing.Optional[ExportedFile]:
        def attached_canceled(*args, **kwargs):
            any_active = False
            for r in self._attached:
                if r.cancelled():
                    continue
                any_active = True
                break
            if any_active:
                return

        self._check_canceled = attached_canceled
        any_active = False
        for r in self._attached:
            if r.cancelled():
                continue
            any_active = True
            r.add_done_callback(self._check_canceled)

        if not any_active:
            self._apply_result(None)
            return None

        async with WorkingDirectory(dir=self._DIRECTORY) as directory:
            exporter = export_data(self.station, self.mode_name, self.export_key,
                                   self.start_epoch_ms, self.end_epoch_ms, directory)
            if not exporter:
                self._apply_result(None)
                return None

            result = await exporter()
            if not result:
                self._apply_result(None)
                return None

            target = NamedTemporaryFile(dir=self._DIRECTORY, buffering=0)
            if isinstance(result, Export.DirectResult):
                try:
                    with open(result.source_file, 'rb') as src:
                        await asyncio.get_event_loop().run_in_executor(_THREAD_POOL, copyfileobj, src, target)
                except OSError:
                    self._apply_result(None)
                    return None
                file = ExportedFile(target, client_name=result.client_name, media_type=result.media_type)
            else:
                await asyncio.get_event_loop().run_in_executor(_THREAD_POOL, self._create_zip, directory, target.name)
                name = result.client_name
                if not name:
                    name = self._export_zip_name()
                elif not name.endswith('.zip'):
                    name += '.zip'
                file = ExportedFile(target, client_name=name)

            self._apply_result(file)
            return file


class Manager:
    _MAXIMUM_AGE = CONFIGURATION.get('EXPORT.RETAINTIME', 15 * 60)

    def __init__(self):
        self._pending: typing.Dict[_ExportKey, _ExportRequest] = OrderedDict()
        self._ready: typing.Dict[_ExportKey, ExportedFile] = dict()
        self._running: typing.Optional[_ExportRequest] = None

    def _start_export(self) -> None:
        if self._running:
            return
        if len(self._pending) <= 0:
            return

        key, export = next(iter(self._pending.items()))
        self._running = export

        async def execute():
            _LOGGER.debug(f"Starting export for {key}")

            result = await export.run()
            if result is not None:
                self._ready[key] = result
            self._pending.pop(key, None)
            self._running = None
            asyncio.get_event_loop().call_soon(self._start_export)

        background_task(execute())

    def __call__(self, station: str, mode_name: str, export_key: str,
                 start_epoch_ms: int, end_epoch_ms: int) -> asyncio.Future:
        key = _ExportKey(station, mode_name, export_key, start_epoch_ms, end_epoch_ms)
        ready = self._ready.get(key)
        if ready:
            ready.accessed = time.monotonic()
            result = asyncio.get_event_loop().create_future()
            result.set_result(ready)
            return result

        pending = self._pending.get(key)
        if pending:
            return pending.attach()

        _LOGGER.debug(f"Queued export for {key}")
        pending = _ExportRequest(station, mode_name, export_key, start_epoch_ms, end_epoch_ms)
        self._pending[key] = pending
        self._start_export()
        return pending.attach()

    def prune(self, all=False):
        evict_time = time.monotonic() - self._MAXIMUM_AGE
        for key in list(self._ready.keys()):
            file = self._ready[key]
            if all or file.accessed < evict_time:
                _LOGGER.debug(f"Removing export entry for {key}")
                del self._ready[key]
