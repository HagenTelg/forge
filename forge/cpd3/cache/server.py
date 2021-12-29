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
from forge.service import UnixServer

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

    def __init__(self, args: typing.List[str]):
        self.created = time.monotonic()
        self.args = args
        self._file = TemporaryFile(dir=self._CACHE_DIRECTORY, buffering=0)
        self._direct_targets: typing.Set[asyncio.StreamWriter] = set()
        self._queued_targets: typing.List[asyncio.StreamWriter] = list()
        self.reader_in_progress = False
        self.reader_complete = False
        self.total_size = 0

    async def _start_reader(self):
        return await asyncio.create_subprocess_exec(self._INTERFACE, *self.args,
                                                    stdout=asyncio.subprocess.PIPE,
                                                    stdin=asyncio.subprocess.DEVNULL)

    async def _read_process(self, writer: asyncio.StreamWriter) -> None:
        _LOGGER.debug(f"Reading process for {self.args}")
        reader = await self._start_reader()
        while True:
            data = await reader.stdout.read(65536)
            if not data:
                break
            try:
                writer.write(data)
                await writer.drain()
            except OSError:
                try:
                    reader.terminate()
                except:
                    pass
                break
        await reader.wait()
        _LOGGER.debug(f"Completed process read for {self.args}")

    async def _read_file(self, writer: asyncio.StreamWriter) -> None:
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
                try:
                    writer.write(chunk)
                    await writer.drain()
                except OSError:
                    break
        finally:
            try:
                os.close(source)
            except OSError:
                pass

        _LOGGER.debug(f"Completed file read for {self.args}")

    async def _send_to_target(self, writer: asyncio.StreamWriter) -> None:
        await self._read_file(writer)
        try:
            writer.close()
        except OSError:
            pass

    async def start(self) -> None:
        _LOGGER.debug(f"Starting initial read for {self.args}")

        reader = await self._start_reader()

        async def run():
            initial_buffer = bytearray()
            while True:
                data = await reader.stdout.read(65536)
                self.reader_in_progress = True
                if not data:
                    break

                if initial_buffer is not None:
                    initial_buffer += data
                    for t in self._queued_targets:
                        try:
                            t.write(bytes(initial_buffer))
                        except OSError:
                            try:
                                t.close()
                            except OSError:
                                pass
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
                    try:
                        t.write(data)
                    except OSError:
                        try:
                            t.close()
                        except OSError:
                            pass
                        self._direct_targets.remove(t)
                        continue
                for t in list(self._direct_targets):
                    try:
                        await t.drain()
                    except OSError:
                        try:
                            t.close()
                        except OSError:
                            pass
                        self._direct_targets.remove(t)
                        continue

                if not self._file and len(self._direct_targets) == 0 and (len(self._queued_targets) == 0 or
                                                                          initial_buffer is None):
                    _LOGGER.debug(f"Aborting canceled read for {self.args}")
                    try:
                        reader.terminate()
                    except:
                        pass
                    break

            self.reader_complete = True
            _LOGGER.debug(f"Completed initial read for {self.args} with {self.total_size} bytes")

            await reader.wait()
            for t in self._direct_targets:
                try:
                    t.close()
                except OSError:
                    pass
            self._direct_targets.clear()

            if self._file:
                os.lseek(self._file.fileno(), 0, os.SEEK_SET)

            for t in self._queued_targets:
                background_task(self._send_to_target(t))
            self._queued_targets.clear()

        background_task(run())

    async def attach(self, writer: asyncio.StreamWriter) -> None:
        if not self._file:
            async def send():
                await self._read_process(writer)
                writer.close()
            background_task(send())
            return
        if not self.reader_in_progress:
            self._direct_targets.add(writer)
            return
        if self.reader_complete:
            background_task(self._send_to_target(writer))
            return
        self._queued_targets.append(writer)

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


async def _cached_read(writer: asyncio.StreamWriter, args: typing.List[str]) -> None:
    key = _CacheKey(args)
    entry = _cache.get(key)
    to_start = None
    if entry is None:
        entry = _CacheEntry(args)
        _cache[key] = entry
        to_start = entry
    await entry.attach(writer)
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


class Server(UnixServer):
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
            await _cached_read(writer, args)
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
