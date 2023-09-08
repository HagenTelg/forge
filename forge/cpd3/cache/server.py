import typing
import os
import asyncio
import logging
import struct
import time
from collections import OrderedDict
from tempfile import TemporaryFile
from dynaconf import Dynaconf
from dynaconf.constants import DEFAULT_SETTINGS_FILES
from forge.tasks import background_task
from forge.service import SocketServer

CONFIGURATION = Dynaconf(
    environments=False,
    lowercase_read=False,
    merge_enabled=True,
    default_settings_paths=DEFAULT_SETTINGS_FILES,
)
_LOGGER = logging.getLogger(__name__)


class _CacheEntry:
    _INTERFACE = CONFIGURATION.get('CPD3.CACHE.INTERFACE', 'cpd3_forge_interface')
    _MAXIMUM_SIZE = CONFIGURATION.get('CPD3.CACHE.ENTRYSIZE', 512 * 1024 * 1024)
    _MAXIMUM_AGE = CONFIGURATION.get('CPD3.CACHE.ENTRYAGE', 15 * 60)
    _CACHE_DIRECTORY = CONFIGURATION.get('CPD3.CACHE.DIRECTORY', '/var/tmp')

    class _WriteTarget:
        def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            self.reader = reader
            self.writer = writer
            self.closed = False
            self._closed_monitor: typing.Optional[asyncio.Task] = background_task(self._monitor_read())

        def _close_writer(self) -> None:
            if not self.writer:
                return
            try:
                self.writer.close()
            except OSError:
                pass
            self.writer = None

        async def _monitor_read(self) -> None:
            while not self.closed:
                data = await self.reader.read(4096)
                if not data:
                    _LOGGER.debug("Connection remote close detected")
                    break
            self.closed = True
            self._closed_monitor = None
            self._close_writer()

        def write(self, data) -> bool:
            if self.closed:
                return False
            try:
                self.writer.write(data)
            except OSError:
                return False
            return True

        async def drain(self) -> bool:
            if self.closed:
                return False
            try:
                await self.writer.drain()
            except OSError:
                return False
            return not self.closed

        def close(self) -> None:
            if self.closed:
                return
            self.closed = True
            t = self._closed_monitor
            self._closed_monitor = None
            if t:
                try:
                    t.cancel()
                except:
                    pass
            self._close_writer()

    def __init__(self, args: typing.List[str]):
        self.created = time.monotonic()
        self.args = args
        self._file = TemporaryFile(dir=self._CACHE_DIRECTORY, buffering=0)
        self._direct_targets: typing.Set[_CacheEntry._WriteTarget] = set()
        self._queued_targets: typing.List[_CacheEntry._WriteTarget] = list()
        self.reader_in_progress = False
        self.reader_complete = False
        self.total_size = 0

    class _MonitoredReader:
        _READ_TIMEOUT = CONFIGURATION.get('CPD3.CACHE.READTIMEOUT', 2 * 60 * 60)

        def __init__(self, reader):
            self.reader = reader
            self._aborted = False
            self._timeout: typing.Optional[asyncio.Task] = background_task(self._run_timeout())

        def _run_termination(self) -> None:
            if not self.reader:
                return
            reader = self.reader
            self.reader = None

            try:
                reader.terminate()
            except:
                pass

            async def _run_kill():
                await asyncio.sleep(60)
                if not reader:
                    return
                try:
                    reader.kill()
                except:
                    pass

            async def _wait_process():
                nonlocal reader
                try:
                    await reader.wait()
                except:
                    pass
                reader = None

            background_task(_wait_process())
            background_task(_run_kill())

        async def _run_timeout(self) -> None:
            await asyncio.sleep(self._READ_TIMEOUT)
            self._aborted = True
            self._timeout = None
            self._run_termination()

        def _cancel_timeout(self) -> None:
            t = self._timeout
            self._timeout = None
            if t is None:
                return
            try:
                t.cancel()
            except:
                pass

        async def read(self) -> bytes:
            if self._aborted or self.reader is None:
                return bytes()
            return await self.reader.stdout.read(65536)

        async def wait(self) -> None:
            if self._aborted or self.reader is None:
                return
            await self.reader.wait()
            self.reader = None
            self._cancel_timeout()

        def abort(self):
            if self._aborted:
                return
            self._aborted = True
            self._cancel_timeout()
            self._run_termination()

    async def _start_reader(self) -> "_CacheEntry._MonitoredReader":
        reader = await asyncio.create_subprocess_exec(self._INTERFACE, *self.args,
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=asyncio.subprocess.DEVNULL)
        return self._MonitoredReader(reader)

    async def _read_process(self, writer: "_CacheEntry._WriteTarget") -> None:
        _LOGGER.debug(f"Reading process for {self.args}")
        reader = await self._start_reader()
        while True:
            data = await reader.read()
            if not data:
                break
            if not writer.write(data) or not await writer.drain():
                reader.abort()
                _LOGGER.debug(f"Aborted process read for {self.args}")
                return
        await reader.wait()
        _LOGGER.debug(f"Completed process read for {self.args}")

    async def _read_file(self, writer: "_CacheEntry._WriteTarget") -> None:
        if not self._file:
            return await self._read_process(writer)
        _LOGGER.debug(f"Reading file for {self.args}")

        try:
            source = os.dup(self._file.fileno())
        except OSError:
            _LOGGER.warning("Error initializing file", exc_info=True)
            return

        try:
            async def reader():
                offset = 0
                while True:
                    data = os.pread(source, 65536, offset)
                    if not data:
                        break
                    offset += len(data)
                    yield data

            async for chunk in reader():
                if not writer.write(chunk) or not await writer.drain():
                    break
        finally:
            try:
                os.close(source)
            except OSError:
                pass

        _LOGGER.debug(f"Completed file read for {self.args}")

    async def _send_to_target(self, writer: "_CacheEntry._WriteTarget") -> None:
        await self._read_file(writer)
        writer.close()

    async def start(self) -> None:
        _LOGGER.debug(f"Starting initial read for {self.args}")

        reader = await self._start_reader()

        async def run():
            initial_buffer = bytearray()
            while True:
                data = await reader.read()
                self.reader_in_progress = True
                if not data:
                    break

                if initial_buffer is not None:
                    initial_buffer += data
                    for t in self._queued_targets:
                        if not t.write(bytes(initial_buffer)):
                            t.close()
                            continue
                        self._direct_targets.add(t)
                    self._queued_targets.clear()

                    if len(initial_buffer) > 65536:
                        initial_buffer = None

                self.total_size += len(data)
                if self._file:
                    if self.total_size > self._MAXIMUM_SIZE:
                        self._file.close()
                        self._file = None
                    else:
                        self._file.write(data)

                for t in list(self._direct_targets):
                    if not t.write(data):
                        t.close()
                        self._direct_targets.remove(t)
                for t in list(self._direct_targets):
                    if not await t.drain():
                        self._direct_targets.remove(t)
                        continue

                if not self._file and len(self._direct_targets) == 0 and (len(self._queued_targets) == 0 or
                                                                          initial_buffer is None):
                    _LOGGER.debug(f"Aborting canceled read for {self.args}")
                    reader.abort()
                    if self._file:
                        self._file.close()
                        self._file = None
                    break

            self.reader_complete = True
            _LOGGER.debug(f"Completed initial read for {self.args} with {self.total_size} bytes")

            await reader.wait()
            for t in self._direct_targets:
                t.close()
            self._direct_targets.clear()

            if self._file:
                os.lseek(self._file.fileno(), 0, os.SEEK_SET)

            for t in self._queued_targets:
                background_task(self._send_to_target(t))
            self._queued_targets.clear()

        background_task(run())

    def attach(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        target = self._WriteTarget(reader, writer)
        if not self._file:
            async def send():
                await self._read_process(target)
                writer.close()
            background_task(send())
            return
        if not self.reader_in_progress:
            self._direct_targets.add(target)
            return
        if self.reader_complete:
            background_task(self._send_to_target(target))
            return
        self._queued_targets.append(target)

    def is_expired(self) -> bool:
        if not self._file:
            return True
        return (time.monotonic() - self.created) > self._MAXIMUM_AGE

    async def evict(self) -> None:
        _LOGGER.debug(f"Removing cache entry for {self.args}")

        if not self._file:
            return

        old = self._file
        self._file = None
        # This would happen automatically at GC, but just do it now, so we release the space sooner
        old.close()


class _CacheKey:
    def __init__(self, args: typing.List[str]):
        self.args = args

    def __eq__(self, other):
        if not isinstance(other, _CacheKey):
            return NotImplemented
        return self.args == other.args

    def __hash__(self):
        return hash(tuple(self.args))

    def __repr__(self):
        return self.args.__repr__()


_cache: typing.Dict[_CacheKey, _CacheEntry] = OrderedDict()


async def _cached_read(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, args: typing.List[str]) -> None:
    key = _CacheKey(args)
    entry = _cache.get(key)
    to_start = None
    if entry is None:
        entry = _CacheEntry(args)
        _cache[key] = entry
        to_start = entry
    entry.attach(reader, writer)
    if to_start:
        await to_start.start()


async def _empty_cache() -> None:
    _LOGGER.debug(f"Clearing cache")
    entries = list(_cache.values())
    _cache.clear()
    for e in entries:
        await e.evict()


async def _prune() -> typing.NoReturn:
    max_entries = CONFIGURATION.get('CPD3.CACHE.MAXENTRIES', 64)
    max_size = _MAXIMUM_SIZE = CONFIGURATION.get('CPD3.CACHE.MAXSIZE', 4 * 1024 * 1024 * 1024)

    while True:
        await asyncio.sleep(30)
        expired: typing.List[_CacheEntry] = []
        cache_size = 0
        for key in list(_cache.keys()):
            entry = _cache[key]
            if entry.is_expired():
                expired.append(entry)
                del _cache[key]
            else:
                cache_size += entry.total_size

        def prune_entry(key):
            nonlocal cache_size
            entry = _cache[key]
            expired.append(entry)
            cache_size -= entry.total_size
            del _cache[key]
            return entry

        if len(_cache) > max_entries:
            total_purge = len(_cache) - max_entries
            for key in list(_cache.keys()):
                prune_entry(key)
                total_purge -= 1
                if total_purge <= 0:
                    break
        if cache_size > max_size:
            total_purge = cache_size - max_size
            for key in list(_cache.keys()):
                total_purge -= prune_entry(key).total_size
                if total_purge <= 0:
                    break

        for e in expired:
            await e.evict()


class Server(SocketServer):
    DESCRIPTION = "Forge CPD3 cache server."

    async def connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        _LOGGER.debug("Accepted connection")
        try:
            n_args = struct.unpack('<I', await reader.readexactly(4))[0]
            args: typing.List[str] = list()
            for i in range(n_args):
                arg_len = struct.unpack('<I', await reader.readexactly(4))[0]
                args.append((await reader.readexactly(arg_len)).decode('utf-8'))
        except (OSError, UnicodeDecodeError, EOFError):
            try:
                writer.close()
            except OSError:
                pass
            return
        _LOGGER.debug(f"Client args {args}")
        if len(args) < 1:
            try:
                writer.close()
            except OSError:
                pass
            return
        operation = args[0]
        if operation == "archive_read" or operation == "edited_read":
            await _cached_read(reader, writer, args)
            return
        elif operation == "directive_create" or operation == "directive_rmw" or operation == "update_passed":
            await _empty_cache()
        try:
            writer.close()
        except OSError:
            pass

    @property
    def default_socket(self) -> str:
        return CONFIGURATION.get('CPD3.CACHE.SOCKET', '/run/forge-cpd3-cache.socket')


def main():
    asyncio.set_event_loop(asyncio.new_event_loop())
    server = Server()
    background_task(_prune())
    server.run()


if __name__ == '__main__':
    main()
