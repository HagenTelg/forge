import asyncio
import typing
import pytest
import os
from forge.acquisition.bus.server.dispatch import Dispatch
from forge.acquisition.bus.client import AcquisitionBusClient
from forge.acquisition.bus.protocol import deserialize_string
from forge.tasks import background_task


class Client(AcquisitionBusClient):
    def __init__(self, source: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(source, reader, writer)
        self.received = asyncio.Queue()

    async def incoming_message(self, source: str, record: str, message: typing.Any) -> None:
        await self.received.put({
            'source': source,
            'record': record,
            'message': message,
        })


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


@pytest.fixture
async def client_to_server():
    return await _aio_pipe()


@pytest.fixture
async def client_from_server():
    return await _aio_pipe()


@pytest.fixture
async def server():
    return Dispatch()


async def _client(server: Dispatch, client_to_server, client_from_server) -> Client:
    client = Client('client', client_from_server[0], client_to_server[1])
    await client.start()
    check = await deserialize_string(client_to_server[0])
    assert check == 'client'
    background_task(server.connection('client', client_to_server[0], client_from_server[1]))
    return client


@pytest.fixture
async def client(server: Dispatch, client_to_server, client_from_server):
    return await _client(server, client_to_server, client_from_server)


@pytest.mark.asyncio
async def test_basic(server: Dispatch, client: Client):
    client.send_data('record1', 'value')
    await client.writer.drain()
    check = await client.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 'value'}

    client.set_state('record2', {'v1': 2.0})
    await client.writer.drain()
    check = await client.received.get()
    assert check == {'source': 'client', 'record': 'record2', 'message': {'v1': 2.0}}

    client.set_source_information('record3', {'v2': 3.0})
    await client.writer.drain()
    check = await client.received.get()
    assert check == {'source': 'client', 'record': 'record3', 'message': {'v2': 3.0}}

    client.set_system_information('record4', 4.0)
    await client.writer.drain()
    check = await client.received.get()
    assert check == {'source': 'client', 'record': 'record4', 'message': 4.0}
    return client


@pytest.mark.asyncio
async def test_basic_persistence(server: Dispatch):
    c1 = await _client(server, await _aio_pipe(), await _aio_pipe())

    c1.send_data('record1', 'value')
    await c1.writer.drain()
    check = await c1.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 'value'}

    c1.set_state('record1', 1.0)
    await c1.writer.drain()
    check = await c1.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 1.0}

    c2 = await _client(server, await _aio_pipe(), await _aio_pipe())
    check = await c2.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 1.0}


@pytest.mark.asyncio
async def test_persistence_levels(server: Dispatch):
    c1 = await _client(server, await _aio_pipe(), await _aio_pipe())

    c1.set_state('record1', 1.0)
    await c1.writer.drain()
    check = await c1.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 1.0}

    c2 = await _client(server, await _aio_pipe(), await _aio_pipe())
    check = await c2.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 1.0}
    await c2.shutdown()

    await c1.shutdown()
    c1 = await _client(server, await _aio_pipe(), await _aio_pipe())
    c1.set_state('record1', 2.0)
    await c1.writer.drain()
    check = await c1.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 2.0}

    c2 = await _client(server, await _aio_pipe(), await _aio_pipe())
    check = await c2.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 2.0}
    await c2.shutdown()

    c1.set_source_information('record1', 3.0)
    await c1.writer.drain()
    check = await c1.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 3.0}

    c2 = await _client(server, await _aio_pipe(), await _aio_pipe())
    check = await c2.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 3.0}
    await c2.shutdown()

    c1.set_system_information('record1', 4.0)
    await c1.writer.drain()
    check = await c1.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 4.0}

    c2 = await _client(server, await _aio_pipe(), await _aio_pipe())
    check = await c2.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 4.0}
    await c2.shutdown()

    await c1.shutdown()
    c2 = await _client(server, await _aio_pipe(), await _aio_pipe())
    check = await c2.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 4.0}
    await c2.shutdown()

    c1 = await _client(server, await _aio_pipe(), await _aio_pipe())
    check = await c1.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 4.0}
    c1.set_state('record1', 5.0)
    await c1.writer.drain()
    check = await c1.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 5.0}

    await c1.shutdown()
    c1 = await _client(server, await _aio_pipe(), await _aio_pipe())
    c1.set_state('record1', 6.0)
    await c1.writer.drain()
    check = await c1.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 6.0}

    c2 = await _client(server, await _aio_pipe(), await _aio_pipe())
    check = await c2.received.get()
    assert check == {'source': 'client', 'record': 'record1', 'message': 6.0}
    await c2.shutdown()

