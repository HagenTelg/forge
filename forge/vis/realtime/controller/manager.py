import typing
import asyncio
import os
import time
import struct
import logging
import bisect
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from .block import ValueType, DataBlock, serialize_single_record


_LOGGER = logging.getLogger(__name__)


class _DataKey:
    def __init__(self, station: str, data_name: str):
        self.station = station.lower()
        self.data_name = data_name.lower()

    def __eq__(self, other):
        if not isinstance(other, _DataKey):
            return NotImplemented
        return self.station == other.station and self.data_name == other.data_name

    def __hash__(self):
        return hash((self.station, self.data_name))

    def __repr__(self):
        return f"({self.station}, {self.data_name})"

    def storage_file(self, storage_directory: Path) -> Path:
        return storage_directory / f'realtime.{self.station}.{self.data_name}'


class _DataEntry:
    class Stream:
        def __init__(self, entry: "_DataEntry", writer: asyncio.StreamWriter):
            self._entry = entry
            self.writer = writer
            self._queue = asyncio.Queue()

        def __enter__(self) -> "_DataEntry.Stream":
            self._entry._streams.add(self)
            return self

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            self._entry._streams.remove(self)

        async def run(self) -> typing.NoReturn:
            while True:
                self.writer.write(await self._queue.get())
                await self.writer.drain()

        def incoming(self, contents: bytes):
            self._queue.put_nowait(contents)

    def __init__(self, storage_file: Path):
        self.storage_file = storage_file
        self.file_lock = asyncio.Lock()
        self._streams: typing.Set["_DataEntry.Stream"] = set()

    def stream_data(self, writer: asyncio.StreamWriter) -> "_DataEntry.Stream":
        return self.Stream(self, writer)

    async def add_record(self, contents: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
        epoch_ms = round(time.time() * 1000)

        async with self.file_lock:
            block = DataBlock()
            try:
                with open(str(self.storage_file), mode='rb') as f:
                    await block.load(f)
            except FileNotFoundError:
                pass

            block.add_record(epoch_ms, contents)

            with open(str(self.storage_file), mode='wb') as f:
                await block.save(f)

        if len(self._streams) == 0:
            return

        stream_data = serialize_single_record(epoch_ms, contents)
        for stream in self._streams:
            stream.incoming(stream_data)

    def is_removable(self) -> bool:
        return len(self._streams) == 0


class Manager:
    _STORAGE_VERSION = 1

    def __init__(self, storage_directory: str):
        self.storage = Path(storage_directory)
        self._data: typing.Dict[_DataKey, _DataEntry] = dict()
        if self._have_sendfile():
            self._sendfile_pool = ThreadPoolExecutor(thread_name_prefix="RealtimeSendfile")
        else:
            self._sendfile_pool = None

    async def load_existing(self):
        try:
            with (self.storage / '.version').open('rt') as f:
                version = int(f.read())
            if version != self._STORAGE_VERSION:
                _LOGGER.debug("Realtime storage version does not match")
                raise ValueError()
        except (FileNotFoundError, ValueError):
            _LOGGER.debug("Initializing realtime storage")
            for file in self.storage.iterdir():
                if file.is_file():
                    try:
                        file.unlink()
                    except FileNotFoundError:
                        pass
            with (self.storage / '.version').open('wt') as f:
                f.write(str(self._STORAGE_VERSION))
            return

        for file in self.storage.iterdir():
            if not file.is_file():
                continue
            parts = file.name.split('.', 2)
            if parts[0] != 'realtime':
                continue

            if file.stat().st_size == 0:
                try:
                    file.unlink()
                except FileNotFoundError:
                    pass
                continue

            try:
                with open(str(file), mode='rb') as f:
                    block = DataBlock()
                    await block.load(f)
            except:
                _LOGGER.debug(f"Unable to load {file}, removing", exc_info=True)
                try:
                    file.unlink()
                except FileNotFoundError:
                    pass
                continue

            station = parts[1]
            date_name = parts[2]
            self._data[_DataKey(station, date_name)] = _DataEntry(file)

        _LOGGER.debug(f"Loaded {len(self._data)} realtime records")

    @staticmethod
    def _have_sendfile() -> bool:
        try:
            _ = os.sendfile
            return True
        except AttributeError:
            pass
        return False

    async def prune(self, maximum_age_ms: int = None, maximum_count: int = None) -> None:
        _LOGGER.debug("Pruning cached data")

        # Insertion/deletion may happen during iteration, so just snapshot the keys
        for key in list(self._data.keys()):
            try:
                entry = self._data[key]
            except KeyError:
                continue

            async with entry.file_lock:
                should_remove = True
                try:
                    def get_n_discard(times: typing.List[int]) -> int:
                        n_records = len(times)
                        n_discard = 0

                        if maximum_count is not None and n_records > maximum_count:
                            n_discard = n_records - maximum_count

                        if maximum_age_ms is not None and (n_records - n_discard) > 0:
                            last_retain_time = int(time.time() * 1000) - maximum_age_ms
                            if last_retain_time > times[-1]:
                                return n_records
                            n_discard = bisect.bisect_left(times, last_retain_time, lo=n_discard)

                        return n_discard

                    try:
                        with open(str(entry.storage_file), mode='r+b') as f:
                            should_remove = await DataBlock.trim(f, get_n_discard)
                    except EOFError:
                        should_remove = True

                    if should_remove:
                        _LOGGER.debug(f"Removing cache entry for {key}")
                        try:
                            os.unlink(str(entry.storage_file))
                        except (OSError, FileNotFoundError):
                            pass
                except FileNotFoundError:
                    should_remove = True

                if should_remove and entry.is_removable():
                    _LOGGER.debug(f"Removing record for {key}")
                    self._data.pop(key, None)

    async def write(self, station: str, data_name: str, reader: asyncio.StreamReader) -> None:
        key = _DataKey(station, data_name)
        entry = self._data.get(key)
        if not entry:
            entry = _DataEntry(key.storage_file(self.storage))
            self._data[key] = entry

        contents: typing.Dict[str, typing.Union[float, typing.List[float]]] = dict()
        n_fields = struct.unpack('<I', await reader.readexactly(4))[0]
        for field_index in range(n_fields):
            field_name_len = struct.unpack('<I', await reader.readexactly(4))[0]
            field_name = (await reader.readexactly(field_name_len)).decode('utf-8')
            field_type = ValueType(struct.unpack('<B', await reader.readexactly(1))[0])

            if field_type == ValueType.FLOAT:
                value: float = struct.unpack('<f', await reader.readexactly(4))[0]
            elif field_type == ValueType.ARRAY_OF_FLOAT:
                value: typing.List[float] = list()
                n_entries = struct.unpack('<I', await reader.readexactly(4))[0]
                raw = await reader.readexactly(4 * n_entries)
                for entry_index in range(n_entries):
                    value.append(struct.unpack_from('<f', raw, entry_index*4)[0])
            elif field_type == ValueType.MISSING:
                continue
            else:
                raise ValueError(f"Unsupported field type {field_type}")

            contents[field_name] = value

        await entry.add_record(contents)

    @staticmethod
    def _get_writer_fileno(writer: asyncio.StreamWriter) -> typing.Optional[int]:
        info = writer.get_extra_info('socket')
        if info:
            return info.fileno()

        info = writer.get_extra_info('pipe')
        if info:
            return info.fileno()

        return None

    async def _send_file_contents(self, source: typing.BinaryIO, writer: asyncio.StreamWriter) -> None:
        if self._sendfile_pool:
            out_fd = self._get_writer_fileno(writer)
            if out_fd is not None:
                in_fd = source.fileno()

                def _send():
                    input_blocking_state = os.get_blocking(in_fd)
                    output_blocking_state = os.get_blocking(out_fd)
                    try:
                        os.set_blocking(in_fd, True)
                        os.set_blocking(out_fd, True)

                        offset = 0
                        remaining = os.fstat(in_fd).st_size
                        while remaining > 0:
                            n = os.sendfile(out_fd, in_fd, offset, min(remaining, 10 * 1024 * 1024))
                            if n <= 0:
                                break
                            offset += n
                            remaining -= n
                    finally:
                        os.set_blocking(out_fd, output_blocking_state)
                        os.set_blocking(in_fd, input_blocking_state)

                _LOGGER.debug("Sending file with os.sendfile")

                await asyncio.wrap_future(self._sendfile_pool.submit(_send))
                await writer.drain()
                return

        _LOGGER.debug("Sending file with a read loop")
        while True:
            data = source.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()

    async def stream(self, station: str, data_name: str, writer: asyncio.StreamWriter) -> typing.NoReturn:
        key = _DataKey(station, data_name)
        entry = self._data.get(key)
        if not entry:
            entry = _DataEntry(key.storage_file(self.storage))
            self._data[key] = entry

        with entry.stream_data(writer) as stream:
            try:
                async with entry.file_lock:
                    with open(str(entry.storage_file), mode='rb') as f:
                        _LOGGER.debug(f"Sending cached data for stream {key}")
                        await self._send_file_contents(f, writer)
            except FileNotFoundError:
                pass

            _LOGGER.debug(f"Streaming {key}")
            try:
                await stream.run()
            except EOFError:
                pass

    async def read(self, station: str, data_name: str, writer: asyncio.StreamWriter) -> None:
        key = _DataKey(station, data_name)
        entry = self._data.get(key)
        if entry is None:
            return
        try:
            async with entry.file_lock:
                with open(str(entry.storage_file), mode='rb') as f:
                    _LOGGER.debug(f"Sending cached data for read {key}")
                    await self._send_file_contents(f, writer)
        except FileNotFoundError:
            pass
