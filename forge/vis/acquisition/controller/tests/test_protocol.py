import asyncio
import typing
import pytest
import os
from forge.vis.acquisition.controller.connection import Station
from forge.vis.acquisition.controller.client import Client as BaseClient
from forge.tasks import background_task


class Client(BaseClient):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)
        self.received = asyncio.Queue()

    async def incoming_data(self, source: str, values: typing.Dict[str, typing.Any]) -> None:
        await self.received.put({
            'type': 'incoming_data',
            'source': source,
            'values': values,
        })

    async def incoming_instrument_add(self, source: str,
                                      information: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
        await self.received.put({
            'type': 'instrument_add',
            'source': source,
            'information': information,
        })

    async def incoming_instrument_update(self, source: str, information: typing.Dict[str, typing.Any]) -> None:
        await self.received.put({
            'type': 'instrument_update',
            'source': source,
            'information': information,
        })

    async def incoming_instrument_remove(self, source: str) -> None:
        await self.received.put({
            'type': 'instrument_remove',
            'source': source,
        })

    async def incoming_instrument_state(self, source: str, state: typing.Dict[str, typing.Any]) -> None:
        await self.received.put({
            'type': 'instrument_state',
            'source': source,
            'state': state,
        })

    async def incoming_event_log(self, source: str, event: typing.Dict[str, typing.Any]) -> None:
        await self.received.put({
            'type': 'event_log',
            'source': source,
            'event': event,
        })

    async def incoming_command(self, target: str, command: str, data: typing.Any) -> None:
        await self.received.put({
            'type': 'command',
            'target': target,
            'command': command,
            'data': data,
        })

    async def incoming_bypass(self, bypassed: bool) -> None:
        await self.received.put({
            'type': 'bypass',
            'bypassed': bypassed,
        })

    async def incoming_message_log(self, author: str, text: str, auxiliary: typing.Any) -> None:
        await self.received.put({
            'type': 'message_log',
            'author': author,
            'text': text,
            'auxiliary': auxiliary,
        })

    async def incoming_restart(self) -> None:
        await self.received.put({
            'type': 'restart',
        })

    async def incoming_chat(self, epoch_ms: int, name: str, text: str) -> None:
        await self.received.put({
            'type': 'chat',
            'epoch_ms': epoch_ms,
            'name': name,
            'text': text,
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
async def acquisition_to_server():
    return await _aio_pipe()


@pytest.fixture
async def acquisition_from_server():
    return await _aio_pipe()


@pytest.fixture
async def display_to_server():
    return await _aio_pipe()


@pytest.fixture
async def display_from_server():
    return await _aio_pipe()


@pytest.fixture
async def server():
    return Station('nil')


@pytest.fixture
async def acquisition(acquisition_to_server, acquisition_from_server, server: Station):
    s = server.attach_acquisition(acquisition_to_server[0], acquisition_from_server[1])
    c = Client(acquisition_from_server[0], acquisition_to_server[1])

    async def run_server():
        try:
            await s.run()
        except EOFError:
            pass

    async def run_client():
        try:
            await c.run()
        except EOFError:
            pass

    background_task(run_server())
    background_task(run_client())

    return s, c


@pytest.fixture
async def display(display_to_server, display_from_server, server: Station):
    s = server.attach_display(display_to_server[0], display_from_server[1])
    c = Client(display_from_server[0], display_to_server[1])

    async def run_server():
        try:
            await s.run()
        except EOFError:
            pass

    async def run_client():
        try:
            await c.run()
        except EOFError:
            pass

    background_task(run_server())
    background_task(run_client())

    return s, c


@pytest.mark.asyncio
async def test_basic(acquisition: typing.Tuple[Station.Connection, Client],
                     display: typing.Tuple[Station.Connection, Client],
                     server: Station):
    s_acquisition, c_acquisition = acquisition
    s_display, c_display = display

    c_acquisition.send_data('S11', {'foo': 1.0})
    await c_acquisition.writer.drain()
    check = await c_display.received.get()
    assert check == {'type': 'incoming_data', 'source': 'S11', 'values': {'foo': 1.0}}

    c_acquisition.send_instrument_add('S11', {'foo': 2.0})
    await c_acquisition.writer.drain()
    check = await c_display.received.get()
    assert check == {'type': 'instrument_add', 'source': 'S11', 'information': {'foo': 2.0}}

    c_acquisition.send_instrument_update('S11', {'foo': 3.0})
    await c_acquisition.writer.drain()
    check = await c_display.received.get()
    assert check == {'type': 'instrument_update', 'source': 'S11', 'information': {'foo': 3.0}}

    c_acquisition.send_instrument_state('S11', {'foo': 4.0})
    await c_acquisition.writer.drain()
    check = await c_display.received.get()
    assert check == {'type': 'instrument_state', 'source': 'S11', 'state': {'foo': 4.0}}

    c_acquisition.send_instrument_remove('S11')
    await c_acquisition.writer.drain()
    check = await c_display.received.get()
    assert check == {'type': 'instrument_remove', 'source': 'S11'}

    c_acquisition.send_event_log('S11', {'foo': 5.0})
    await c_acquisition.writer.drain()
    check = await c_display.received.get()
    assert check == {'type': 'event_log', 'source': 'S11', 'event': {'foo': 5.0}}

    c_display.send_command('S11', 'bar', 'foobar')
    await c_display.writer.drain()
    check = await c_acquisition.received.get()
    assert check == {'type': 'command', 'target': 'S11', 'command': 'bar', 'data': 'foobar'}

    c_display.send_bypass(True)
    await c_display.writer.drain()
    check = await c_acquisition.received.get()
    assert check == {'type': 'bypass', 'bypassed': True}

    c_display.send_message_log('from name', 'text data', {'foo': 6.0})
    await c_display.writer.drain()
    check = await c_acquisition.received.get()
    assert check == {'type': 'message_log', 'author': 'from name', 'text': 'text data', 'auxiliary': {'foo': 6.0}}

    c_display.send_restart()
    await c_display.writer.drain()
    check = await c_acquisition.received.get()
    assert check == {'type': 'restart'}

    c_display.send_chat(123, 'from name', 'text data')
    await c_display.writer.drain()
    check = await c_acquisition.received.get()
    assert check == {'type': 'chat', 'epoch_ms': 123, 'name': 'from name', 'text': 'text data'}


@pytest.mark.asyncio
async def test_display_disconnect(acquisition: typing.Tuple[Station.Connection, Client],
                                  display: typing.Tuple[Station.Connection, Client],
                                  server: Station):
    s_acquisition, c_acquisition = acquisition
    s_display, c_display = display

    c_acquisition.send_data('S11', {'foo': 1.0})
    await c_acquisition.writer.drain()
    check = await c_display.received.get()
    assert check == {'type': 'incoming_data', 'source': 'S11', 'values': {'foo': 1.0}}

    completed = server.detach(s_display)
    assert not completed

    c_acquisition.send_data('S11', {'foo': 1.0})
    await c_acquisition.writer.drain()

    completed = server.detach(s_acquisition)
    assert completed


@pytest.mark.asyncio
async def test_acquisition_disconnect(acquisition: typing.Tuple[Station.Connection, Client],
                                      display: typing.Tuple[Station.Connection, Client],
                                      server: Station):
    s_acquisition, c_acquisition = acquisition
    s_display, c_display = display

    c_display.send_chat(123, 'from name', 'text data')
    await c_display.writer.drain()
    check = await c_acquisition.received.get()
    assert check == {'type': 'chat', 'epoch_ms': 123, 'name': 'from name', 'text': 'text data'}

    completed = server.detach(s_acquisition)
    assert not completed

    completed = server.detach(s_display)
    assert completed


@pytest.mark.asyncio
async def test_existing_send(acquisition: typing.Tuple[Station.Connection, Client], server: Station):
    s_acquisition, c_acquisition = acquisition

    c_acquisition.send_instrument_add('S11', {'foo': 2})
    c_acquisition.send_data('S11', {'foo': 1.0})
    await c_acquisition.writer.drain()

    display_to_server = await _aio_pipe()
    display_from_server = await _aio_pipe()
    s_display = server.attach_display(display_to_server[0], display_from_server[1])
    c_display = Client(display_from_server[0], display_to_server[1])

    async def run_server():
        try:
            await s_display.run()
        except EOFError:
            pass

    async def run_client():
        try:
            await c_display.run()
        except EOFError:
            pass

    background_task(run_server())
    background_task(run_client())

    check = await c_display.received.get()
    assert check == {'type': 'instrument_add', 'source': 'S11', 'information': {'foo': 2.0}}
    check = await c_display.received.get()
    assert check == {'type': 'incoming_data', 'source': 'S11', 'values': {'foo': 1.0}}
