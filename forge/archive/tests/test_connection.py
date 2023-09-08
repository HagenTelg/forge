import pytest
import pytest_asyncio
import asyncio
import typing
import os
from tempfile import NamedTemporaryFile
from forge.archive import CONFIGURATION
from forge.archive.server.control import Controller
from forge.archive.client.connection import Connection, LockDenied


CONFIGURATION.set('ARCHIVE.LOCK_STORAGE', False)


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


@pytest_asyncio.fixture
async def control(tmp_path):
    dest = tmp_path / "storage"
    dest.mkdir(exist_ok=True)
    c = Controller(dest)
    await c.initialize()
    return c


async def _make_connection(control: Controller) -> typing.Tuple[asyncio.Task, asyncio.Task, Connection]:
    client_reader, server_writer = await _aio_pipe()
    server_reader, client_writer = await _aio_pipe()

    control_run = asyncio.ensure_future(control.connection(server_reader, server_writer))
    connection = Connection(client_reader, client_writer, repr(client_reader))
    await connection.initialize()
    connection_run = asyncio.ensure_future(connection.run())
    return control_run, connection_run, connection


@pytest_asyncio.fixture
async def control_connection(control):
    return await _make_connection(control)


@pytest.mark.asyncio
async def test_basic(control, control_connection):
    control_run, connection_run, connection = control_connection

    await connection.shutdown()
    await connection_run
    await control_run


@pytest.mark.asyncio
async def test_storage(control, control_connection):
    control_run, connection_run, connection = control_connection

    await connection.transaction_begin(False)
    await connection.transaction_commit()
    await connection.transaction_begin(True)
    await connection.set_transaction_status("Doing stuff")
    await connection.transaction_abort()

    async with connection.transaction(True):
        await connection.lock_write("test/data", 100, 200)
        await connection.write_bytes("test/file1", b"TestBytes")

        with NamedTemporaryFile() as f:
            f.write(b"TestFile")
            f.seek(0)
            await connection.write_file("test/file2", f)

        data = b"TestData"
        offset = 0

        async def reader(count: int):
            nonlocal offset
            begin = offset
            offset += count
            return data[begin:begin+count]

        await connection.write_data("test/file3", len(data), reader)

    await connection.transaction_begin(True)
    await connection.lock_write("test/data", 100, 200)
    await connection.write_bytes("test/file1", b"ABORTED")
    await connection.transaction_abort()

    async with connection.transaction(False):
        await connection.lock_read("test/data", 100, 500)

        assert await connection.read_bytes("test/file3") == b"TestData"

        data = bytearray()

        async def writer(d: bytes):
            nonlocal data
            data += d

        await connection.read_data("test/file2", writer)
        assert data == b"TestFile"

        with NamedTemporaryFile() as f:
            await connection.read_file("test/file1", f)
            f.seek(0)
            assert f.read() == b"TestBytes"

        try:
            await connection.read_bytes("test/INVALID")
            assert False
        except FileNotFoundError:
            pass

    async with connection.transaction(True):
        await connection.lock_read("test/index", 100, 500)
        await connection.lock_write("test/data", 100, 200)

        assert await connection.read_bytes("test/file1") == b"TestBytes"
        assert await connection.read_bytes("test/file2") == b"TestFile"
        assert await connection.read_bytes("test/file3") == b"TestData"

        await connection.write_bytes("test/file1", b"Updated")
        await connection.remove_file("test/file3")

        assert await connection.read_bytes("test/file1") == b"Updated"
        try:
            await connection.read_bytes("test/file3")
            assert False
        except FileNotFoundError:
            pass

    async with connection.transaction(False):
        assert await connection.read_bytes("test/file1") == b"Updated"
        try:
            await connection.read_bytes("test/file3")
            assert False
        except FileNotFoundError:
            pass

    assert sorted(await connection.list_files("test")) == ["test/file1", "test/file2"]

    await connection.shutdown()
    await connection_run
    await control_run


@pytest.mark.asyncio
async def test_overlap(control):
    control1_run, connection1_run, connection1 = await _make_connection(control)
    control2_run, connection2_run, connection2 = await _make_connection(control)

    async with connection1.transaction(True):
        await connection1.lock_write("test/key1", 100, 200)
        await connection1.lock_write("test/key1", 50, 150)
        await connection1.lock_read("test/key1", 75, 125)
        await connection1.set_transaction_status("STATUS")
        async with connection2.transaction(False):
            await connection2.lock_read("test/key2", 100, 200)
            await connection2.lock_read("test/key1", 300, 400)
            try:
                await connection2.lock_read("test/key1", 100, 200)
                assert False
            except LockDenied as e:
                assert e.status == "STATUS"

    async with connection2.transaction(False):
        await connection2.lock_read("test/key1", 100, 200)
        async with connection1.transaction(True):
            await connection1.lock_write("test/key1", 100, 200)

    async with connection2.transaction(False):
        await connection2.lock_read("test/key1", 100, 200)
        async with connection1.transaction(False):
            await connection1.lock_read("test/key1", 100, 200)

    notifications: typing.List[typing.Tuple[str, int, int]] = list()

    async def notify_received(key: str, start: int, end: int) -> None:
        notifications.append((key, start, end))

    await connection1.listen_notification("test/notify1", notify_received)
    async with connection1.transaction(True):
        await connection1.lock_write("test/key1", 100, 200)
        await connection1.set_transaction_status("STATUS")
        async with connection2.transaction(True):
            try:
                await connection2.lock_write("test/key1", 100, 200)
                assert False
            except LockDenied as e:
                assert e.status == "STATUS"
            await connection2.send_notification("test/notify1", 100, 200)
            await connection2.send_notification("test/notify2", 100, 200)
    assert notifications == [("test/notify1", 100, 200)]
    notifications.clear()

    await connection2.transaction_begin(True)
    await connection2.send_notification("test/notify1", 100, 200)
    await connection2.transaction_abort()
    assert notifications == []

    await connection1.listen_notification("test/notify2", notify_received)
    async with connection2.transaction(True):
        await connection2.send_notification("test/notify1", 100, 200)
        await connection2.send_notification("test/notify2", 100, 200)
    assert notifications == [("test/notify1", 100, 200), ("test/notify2", 100, 200)]
    notifications.clear()

    hit_intents: typing.List[typing.Tuple[str, int, int]] = list()

    async def intent_hit(key: str, start: int, end: int) -> None:
        hit_intents.append((key, start, end))

    await connection1.listen_intent("test/key1", intent_hit)
    intent1 = await connection1.acquire_intent("test/key1", 100, 200)
    await connection1.transaction_begin(True)
    intent2 = await connection1.acquire_intent("test/key1", 200, 300)
    acquired = await connection1.transaction_commit()
    assert acquired == [intent2]
    await connection1.transaction_begin(True)
    intent3 = await connection1.acquire_intent("test/key1", 300, 400)
    await intent2.release()
    unreleased = await connection1.transaction_abort()
    assert unreleased == [intent2]
    async with connection1.transaction(True):
        await connection1.lock_write("test/key1", 100, 200)
    assert hit_intents == []

    async with connection2.transaction(True):
        try:
            await connection2.lock_read("test/key1", 100, 200)
            assert False
        except LockDenied:
            pass
        try:
            await connection2.lock_write("test/key1", 250, 275)
            assert False
        except LockDenied:
            pass
        await connection2.lock_write("test/key1", 350, 400)

    # Also sync the intents
    async with connection2.transaction(True):
        await intent2.release()

    assert hit_intents == [("test/key1", 100, 200), ("test/key1", 250, 275)]
    hit_intents.clear()

    intent3 = await connection1.acquire_intent("test/key1", 300, 400)
    await intent3.release()

    async with connection2.transaction(True):
        await connection2.lock_write("test/key1", 250, 275)
        await connection2.lock_write("test/key1", 350, 400)
    assert hit_intents == []

    await connection1.shutdown()
    await connection1_run
    await control1_run
    intent1._realized = False

    async with connection2.transaction(True):
        await connection2.lock_write("test/key1", 100, 200)

    await connection2.shutdown()
    await connection2_run
    await control2_run
