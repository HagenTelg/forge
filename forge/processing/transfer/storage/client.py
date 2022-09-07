import typing
import asyncio
import struct
import zstandard
from abc import ABC, abstractmethod
from forge.crypto import PublicKey, key_to_bytes
from forge.processing.transfer.storage.protocol import ServerConnectionType, FileType, Compression, AddFileOperation, GetFileOperation


class WriteFile:
    def __init__(self, station: str, key: PublicKey, filename: str):
        self.station = station
        self.key = key
        self.filename = filename
        self.file_type: FileType = FileType.DATA
        self.compression: Compression = Compression.NONE
        self.reader: typing.Optional[asyncio.StreamReader] = None
        self.writer: typing.Optional[asyncio.StreamWriter] = None

    async def connect(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> bool:
        def send_string(s: str):
            raw = s.encode('utf-8')
            writer.write(struct.pack('<I', len(raw)))
            writer.write(raw)

        writer.write(struct.pack('<B', ServerConnectionType.ADD_FILE.value))
        writer.write(struct.pack('<B', self.file_type.value))
        writer.write(key_to_bytes(self.key))
        send_string(self.filename)
        send_string(self.station)
        writer.write(struct.pack('<B', self.compression.value))

        await writer.drain()
        ok = struct.unpack('<B', await reader.readexactly(1))[0]
        if not ok:
            writer.close()
            return False

        self.reader = reader
        self.writer = writer
        return True

    async def write_chunk(self, data: bytes) -> None:
        while data:
            send = data[:0xFFFF]
            data = data[0xFFFF:]
            self.writer.write(struct.pack('<BH', AddFileOperation.CHUNK.value, len(send)))
            self.writer.write(send)
            await self.writer.drain()
            ok = struct.unpack('<B', await self.reader.readexactly(1))[0]
            if not ok:
                self.writer.close()
                self.writer = None
                self.reader = None
                raise ValueError("chunk not acknowledged")

    async def abort(self) -> None:
        if self.writer:
            self.writer.write(struct.pack('<B', AddFileOperation.ABORT.value))
            await self.writer.drain()
            self.writer.close()
        self.reader = None

    async def complete(self) -> bool:
        if self.writer:
            self.writer.write(struct.pack('<B', AddFileOperation.COMPLETE.value))
            await self.writer.drain()
            ok = struct.unpack('<B', await self.reader.readexactly(1))[0]
            self.writer.close()
            self.writer = None
            self.reader = None
            return ok != 0
        return False


class GetFiles:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer

    class FetchFile(ABC):
        def __init__(self, station: str, key: PublicKey, filename: str, file_type: FileType, compression: Compression):
            self.station = station.lower()
            self.key = key
            self.filename = filename
            self.file_type = file_type
            self.compression = compression

        @abstractmethod
        async def begin_fetch(self) -> typing.Optional[typing.BinaryIO]:
            pass

        async def complete(self, output: typing.BinaryIO, ok: bool) -> None:
            output.close()

    async def run(self) -> None:
        self.writer.write(struct.pack('<B', ServerConnectionType.GET_FILES.value))
        await self.writer.drain()

        async def string_arg() -> str:
            arg_len = struct.unpack('<I', await self.reader.readexactly(4))[0]
            return (await self.reader.readexactly(arg_len)).decode('utf-8')

        while True:
            key = PublicKey.from_public_bytes(await self.reader.readexactly(32))
            file_type = FileType(struct.unpack('<B', await self.reader.readexactly(1))[0])
            filename = await string_arg()
            station = await string_arg()
            compression = Compression(struct.unpack('<B', await self.reader.readexactly(1))[0])

            file = self.FetchFile(station, key, filename, file_type, compression)
            output = await file.begin_fetch()
            if not output:
                self.writer.write(struct.pack('<B', 0))
                await self.writer.drain()
                continue
            try:
                self.writer.write(struct.pack('<B', 1))
                await self.writer.drain()

                close_chunk_output = False
                if file.compression == Compression.NONE:
                    chunk_output = output
                elif file.compression == Compression.ZSTD:
                    chunk_output = zstandard.ZstdDecompressor().stream_writer(output, closefd=False)
                    close_chunk_output = True
                else:
                    raise ValueError("invalid compression type")

                while True:
                    op = GetFileOperation(struct.unpack('<B', await self.reader.readexactly(1))[0])
                    if op == GetFileOperation.CHUNK:
                        chunk_size = struct.unpack('<H', await self.reader.readexactly(2))[0]
                        chunk_data = await self.reader.readexactly(chunk_size)

                        chunk_output.write(chunk_data)

                        self.writer.write(struct.pack('<B', 1))
                        await self.writer.drain()
                    elif op == GetFileOperation.COMPLETE:
                        break
                    elif op == GetFileOperation.ABORT:
                        if close_chunk_output:
                            chunk_output.close()
                        await file.complete(output, False)
                        output = None
                        return

                if close_chunk_output:
                    chunk_output.close()

                await file.complete(output, True)
                output = None

                self.writer.write(struct.pack('<B', 1))
                await self.writer.drain()
            finally:
                if output:
                    await file.complete(output, False)


if __name__ == '__main__':
    import argparse
    from base64 import b64encode
    from forge.processing.transfer import CONFIGURATION

    parser = argparse.ArgumentParser(description="Data transfer test client.")
    parser.add_argument('--socket',
                        dest='socket', type=str,
                        default=CONFIGURATION.get('PROCESSING.TRANSFER.SOCKET', '/run/forge-transfer-storage.socket'),
                        help="server socket")
    args = parser.parse_args()

    class _Client(GetFiles):
        class FetchFile(GetFiles.FetchFile):
            async def begin_fetch(self) -> typing.Optional[typing.BinaryIO]:
                key = b64encode(key_to_bytes(self.key)).decode('ascii')
                print(f"{key} {self.station} {self.filename} {self.file_type.name}")
                return None

    async def run():
        reader, writer = await asyncio.open_unix_connection(args.socket)
        client = _Client(reader, writer)
        await client.run()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.close()
