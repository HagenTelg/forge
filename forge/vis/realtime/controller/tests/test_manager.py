import asyncio
import typing
import pytest
import pytest_asyncio
import os
import time
from forge.vis.realtime.controller.manager import Manager
from forge.vis.realtime.controller.block import DataBlock, serialize_single_record


@pytest_asyncio.fixture
async def manager(tmp_path):
    manager = Manager(str(tmp_path))
    await manager.load_existing()
    return manager


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
async def to_manager():
    return await _aio_pipe()


@pytest_asyncio.fixture
async def from_manager():
    return await _aio_pipe()


async def _write_record(writer: asyncio.StreamWriter,
                        record: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
    data = serialize_single_record(0, record)
    data = data[12:]  # Count and time
    writer.write(data)
    await writer.drain()


async def _read_blocks(reader: asyncio.StreamReader, count: int) -> typing.List[DataBlock]:
    result: typing.List[DataBlock] = list()
    for i in range(count):
        block = DataBlock()
        await block.load(reader)
        result.append(block)
    return result


@pytest.mark.asyncio
async def test_basic(manager, tmp_path, to_manager, from_manager):
    assert (tmp_path / '.version').exists()

    m_reader, m_writer = to_manager
    c_reader, c_writer = from_manager

    write_time = time.time()
    write_task = asyncio.ensure_future(_write_record(m_writer, {'foo': 1.0}))
    await manager.write('nil', 'data', m_reader)
    await write_task

    assert (tmp_path / 'realtime.nil.data').exists()

    read_task = asyncio.ensure_future(_read_blocks(c_reader, 1))
    await manager.read('nil', 'data', c_writer)
    blocks = await read_task

    assert len(blocks) == 1
    assert len(blocks[0].records) == 1
    assert blocks[0].records[0].epoch_ms >= int(write_time * 1000)
    assert blocks[0].records[0].fields == {'foo': 1.0}


@pytest.mark.asyncio
async def test_stream(manager, to_manager, from_manager):
    m_reader, m_writer = to_manager
    c_reader, c_writer = from_manager

    write_task = asyncio.ensure_future(_write_record(m_writer, {'foo': 1.0}))
    await manager.write('nil', 'data', m_reader)
    await write_task

    read_task = asyncio.ensure_future(_read_blocks(c_reader, 1))
    stream_task = asyncio.ensure_future(manager.stream('nil', 'data', c_writer))

    blocks = await read_task
    assert len(blocks) == 1
    assert len(blocks[0].records) == 1
    assert blocks[0].records[0].fields == {'foo': 1.0}

    read_task = asyncio.ensure_future(_read_blocks(c_reader, 2))

    write_task = asyncio.ensure_future(_write_record(m_writer, {'foo': 2.0}))
    await manager.write('nil', 'data', m_reader)
    await write_task

    write_task = asyncio.ensure_future(_write_record(m_writer, {'foo': 3.0}))
    await manager.write('nil', 'data', m_reader)
    await write_task

    blocks = await read_task
    stream_task.cancel()
    try:
        await stream_task
    except asyncio.CancelledError:
        pass

    assert len(blocks) == 2
    assert len(blocks[0].records) == 1
    assert blocks[0].records[0].fields == {'foo': 2.0}
    assert len(blocks[1].records) == 1
    assert blocks[1].records[0].fields == {'foo': 3.0}


@pytest.mark.asyncio
async def test_prune(manager, tmp_path, to_manager):
    m_reader, m_writer = to_manager

    write_task = asyncio.ensure_future(_write_record(m_writer, {'foo': 1.0}))
    await manager.write('nil', 'data', m_reader)
    await write_task
    assert (tmp_path / 'realtime.nil.data').exists()

    await manager.prune(maximum_count=0)
    assert not (tmp_path / 'realtime.nil.data').exists()

    write_task = asyncio.ensure_future(_write_record(m_writer, {'foo': 1.0}))
    await manager.write('nil', 'data', m_reader)
    await write_task
    assert (tmp_path / 'realtime.nil.data').exists()

    await manager.prune(maximum_age_ms=-10000)
    assert not (tmp_path / 'realtime.nil.data').exists()
