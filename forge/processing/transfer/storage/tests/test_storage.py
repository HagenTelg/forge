import asyncio
import typing
import pytest
import os
import random
import struct
from io import BytesIO
from forge.crypto import PrivateKey, PublicKey, key_to_bytes
from forge.processing.transfer.storage.protocol import FileType, Compression
from forge.processing.transfer.storage.dispatch import Dispatch
from forge.processing.transfer.storage.client import WriteFile, GetFiles
from forge.processing.transfer.storage.protocol import ServerConnectionType


@pytest.fixture
def private_key():
    contents = bytearray()
    for i in range(32):
        contents.append(random.randrange(0x100))
    return PrivateKey.from_private_bytes(contents)


@pytest.fixture
def public_key(private_key):
    return private_key.public_key()


async def _aio_pipe() -> typing.Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    read, write = os.pipe()
    read = os.fdopen(read, mode='rb')
    write = os.fdopen(write, mode='wb')

    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), read)

    transport, protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, write)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)

    return reader, writer


async def _accept_add(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, write_file: WriteFile):
    async def string_arg() -> str:
        arg_len = struct.unpack('<I', await reader.readexactly(4))[0]
        return (await reader.readexactly(arg_len)).decode('utf-8')

    connection_type = ServerConnectionType(struct.unpack('<B', await reader.readexactly(1))[0])
    assert connection_type == ServerConnectionType.ADD_FILE

    file_type = FileType(struct.unpack('<B', await reader.readexactly(1))[0])
    assert file_type == write_file.file_type
    key = PublicKey.from_public_bytes(await reader.readexactly(32))
    assert key_to_bytes(key) == key_to_bytes(write_file.key)
    filename = await string_arg()
    assert filename == write_file.filename
    station = await string_arg()
    assert station == write_file.station
    compression = Compression(struct.unpack('<B', await reader.readexactly(1))[0])
    assert compression == write_file.compression


async def _accept_get(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, get_files: GetFiles):
    connection_type = ServerConnectionType(struct.unpack('<B', await reader.readexactly(1))[0])
    assert connection_type == ServerConnectionType.GET_FILES


@pytest.mark.asyncio
async def test_add(public_key):
    dispatch = Dispatch()

    get_reader, cl_get_writer = await _aio_pipe()
    cl_get_reader, get_writer = await _aio_pipe()

    write = WriteFile("nil", public_key, "test_file")

    get_received = asyncio.Event()

    class TestGet(GetFiles):
        class FetchFile(GetFiles.FetchFile):
            async def begin_fetch(self) -> typing.Optional[typing.BinaryIO]:
                assert self.station == write.station
                assert key_to_bytes(self.key) == key_to_bytes(write.key)
                assert self.filename == write.filename
                assert self.file_type == write.file_type
                return BytesIO()

            async def complete(self, output: BytesIO, ok: bool) -> None:
                assert ok
                assert output.getvalue() == b"12345"
                get_received.set()

    get = TestGet(cl_get_reader, cl_get_writer)
    get_client_task = asyncio.ensure_future(get.run())
    await _accept_get(get_reader, get_writer, get)
    await dispatch.connection(get_reader, get_writer)

    add_reader, cl_add_writer = await _aio_pipe()
    cl_add_reader, add_writer = await _aio_pipe()

    write_connect = asyncio.ensure_future(write.connect(cl_add_reader, cl_add_writer))
    await _accept_add(add_reader, add_writer, write)
    add_task = asyncio.ensure_future(dispatch.add(
        add_reader, add_writer,
        write.key, write.file_type, write.filename, write.station, write.compression
    ))

    ok = await write_connect
    assert ok
    await write.write_chunk(b"12345")
    await write.complete()

    await add_task

    await get_received.wait()
    get_client_task.cancel()
    try:
        await get_client_task
    except asyncio.CancelledError:
        pass
    get.writer.close()


@pytest.mark.asyncio
async def test_no_target(public_key):
    dispatch = Dispatch()

    add_reader, cl_add_writer = await _aio_pipe()
    cl_add_reader, add_writer = await _aio_pipe()

    write = WriteFile("nil", public_key, "test_file")

    write_connect = asyncio.ensure_future(write.connect(cl_add_reader, cl_add_writer))
    await _accept_add(add_reader, add_writer, write)
    add_task = asyncio.ensure_future(dispatch.add(
        add_reader, add_writer,
        write.key, write.file_type, write.filename, write.station, write.compression
    ))

    ok = await write_connect
    assert not ok

    await add_task


@pytest.mark.asyncio
async def test_abort(public_key):
    dispatch = Dispatch()

    get_reader, cl_get_writer = await _aio_pipe()
    cl_get_reader, get_writer = await _aio_pipe()

    write = WriteFile("nil", public_key, "test_file")

    get_received = asyncio.Event()

    class TestGet(GetFiles):
        class FetchFile(GetFiles.FetchFile):
            async def begin_fetch(self) -> typing.Optional[typing.BinaryIO]:
                assert self.station == write.station
                assert key_to_bytes(self.key) == key_to_bytes(write.key)
                assert self.filename == write.filename
                assert self.file_type == write.file_type
                return BytesIO()

            async def complete(self, output: BytesIO, ok: bool) -> None:
                assert not ok
                get_received.set()

    get = TestGet(cl_get_reader, cl_get_writer)
    get_client_task = asyncio.ensure_future(get.run())
    await _accept_get(get_reader, get_writer, get)
    await dispatch.connection(get_reader, get_writer)

    add_reader, cl_add_writer = await _aio_pipe()
    cl_add_reader, add_writer = await _aio_pipe()

    write_connect = asyncio.ensure_future(write.connect(cl_add_reader, cl_add_writer))
    await _accept_add(add_reader, add_writer, write)
    add_task = asyncio.ensure_future(dispatch.add(
        add_reader, add_writer,
        write.key, write.file_type, write.filename, write.station, write.compression
    ))

    ok = await write_connect
    assert ok
    await write.write_chunk(b"12345")
    await write.abort()

    await add_task

    await get_received.wait()
    get_client_task.cancel()
    try:
        await get_client_task
    except asyncio.CancelledError:
        pass
    get.writer.close()
