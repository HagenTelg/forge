import os
import typing
import asyncio
import logging
import struct
from forge.crypto import PublicKey, key_to_bytes
from .protocol import FileType, Compression, AddFileOperation, GetFileOperation


_LOGGER = logging.getLogger(__name__)


class Dispatch:
    class _GetConnection:
        def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            self.reader = reader
            self.writer = writer

        def _send_string(self, s: str):
            raw = s.encode('utf-8')
            self.writer.write(struct.pack('<I', len(raw)))
            self.writer.write(raw)

        async def should_handle(self, key: PublicKey, file_type: FileType, filename: str, station: str,
                                compression: Compression) -> bool:
            self.writer.write(key_to_bytes(key))
            self.writer.write(struct.pack('<B', file_type.value))
            self._send_string(filename)
            self._send_string(station)
            self.writer.write(struct.pack('<B', compression.value))
            await self.writer.drain()
            return (struct.unpack('<B', await self.reader.readexactly(1))[0]) != 0

        async def send_chunk(self, chunk: bytes) -> bool:
            self.writer.write(struct.pack('<BH', GetFileOperation.CHUNK.value, len(chunk)))
            self.writer.write(chunk)
            await self.writer.drain()
            ok = struct.unpack('<B', await self.reader.readexactly(1))[0]
            if not ok:
                raise ValueError("unacknowledged chunk during file send")
            return True

        async def send_abort(self) -> None:
            self.writer.write(struct.pack('<B', GetFileOperation.ABORT.value))

        async def send_complete(self) -> bool:
            self.writer.write(struct.pack('<B', GetFileOperation.COMPLETE.value))
            await self.writer.drain()
            ok = struct.unpack('<B', await self.reader.readexactly(1))[0]
            if not ok:
                raise ValueError("file send complete not acknowledged")
            return True

    def __init__(self):
        self._lock = asyncio.Lock()
        self._get: typing.Set[Dispatch._GetConnection] = set()

    async def add(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                  key: PublicKey, file_type: FileType,
                  filename: str, station: str, file_compression: Compression) -> None:
        async def client_operation(c, op: typing.Awaitable):
            try:
                return await op
            except EOFError:
                _LOGGER.debug(f"Connection closing", exc_info=True)
                self._get.remove(c)
                try:
                    c.writer.close()
                except OSError:
                    pass
            except:
                _LOGGER.warning(f"Error sending file {filename}", exc_info=True)
                self._get.remove(c)
                try:
                    c.writer.close()
                except OSError:
                    pass
            return None

        async with self._lock:
            targets: typing.List[Dispatch._GetConnection] = list()

            for c in list(self._get):
                if not await client_operation(c, c.should_handle(key, file_type, filename, station, file_compression)):
                    continue
                targets.append(c)

            if len(targets) == 0:
                _LOGGER.warning(f"No suitable target for {station}/{filename}")
                writer.write(struct.pack('<B', 0))
                await writer.drain()
                return

            writer.write(struct.pack('<B', 1))

            total_size = 0
            while True:
                op = AddFileOperation(struct.unpack('<B', await reader.readexactly(1))[0])
                if op == AddFileOperation.CHUNK:
                    chunk_size = struct.unpack('<H', await reader.readexactly(2))[0]
                    chunk_data = await reader.readexactly(chunk_size)
                    total_size += chunk_size

                    ok = await asyncio.gather(*[
                        client_operation(c, c.send_chunk(chunk_data))
                        for c in targets
                    ])
                    if None in ok:
                        _LOGGER.warning(f"Dispatch failed for {station}/{filename} after {total_size} bytes")
                        writer.write(struct.pack('<B', 0))
                        await writer.drain()
                        for c in targets:
                            await client_operation(c, c.send_abort())
                        return

                    writer.write(struct.pack('<B', 1))
                    await writer.drain()
                elif op == AddFileOperation.COMPLETE:
                    break
                elif op == AddFileOperation.ABORT:
                    _LOGGER.debug(f"Receive abort for {station}/{filename} after {total_size} bytes")
                    for c in targets:
                        await client_operation(c, c.send_abort())
                    return

            ok = await asyncio.gather(*[
                client_operation(c, c.send_complete())
                for c in targets
            ])
            if None in ok:
                _LOGGER.warning(f"Dispatch failed for {station}/{filename} during completion with {total_size} bytes")
                writer.write(struct.pack('<B', 0))
                await writer.drain()
                return

            _LOGGER.debug(f"Dispatch complete for {station}/{filename} with {total_size} bytes")
            writer.write(struct.pack('<B', 1))
            await writer.drain()

    async def prune_connections(self):
        while True:
            await asyncio.sleep(10)
            async with self._lock:
                for c in list(self._get):
                    if c.reader.at_eof():
                        _LOGGER.debug("Connection closed")
                        self._get.remove(c)
                        try:
                            c.writer.close()
                        except:
                            pass

    async def connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        c = self._GetConnection(reader, writer)
        async with self._lock:
            self._get.add(c)
