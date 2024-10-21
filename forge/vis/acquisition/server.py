import typing
import asyncio
import logging
import starlette.status
import struct
import time
import ipaddress
from math import isfinite, nan
from base64 import b64encode
from starlette.routing import BaseRoute, Route, WebSocketRoute
from starlette.datastructures import URL
from starlette.authentication import requires
from starlette.endpoints import WebSocketEndpoint
from starlette.exceptions import HTTPException
from starlette.websockets import WebSocket
from forge.const import STATIONS
from forge.vis import CONFIGURATION
from forge.vis.util import sanitize_for_json
from .assemble import display, summary
from .controller.client import Client as BaseClient

_LOGGER = logging.getLogger(__name__)


class _AcquisitionSocket(WebSocketEndpoint):
    encoding = 'json'

    class Client(BaseClient):
        _MVC_FLOAT = struct.pack('<f', nan)

        def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, websocket: WebSocket):
            super().__init__(reader, writer)
            self.websocket = websocket

        async def incoming_data(self, source: str, values: typing.Dict[str, typing.Any]) -> None:
            if len(values) == 0:
                return

            generic: typing.Dict[str, typing.Any] = dict()
            simple: typing.Dict[str, float] = dict()
            arrays: typing.Dict[str, typing.List[float]] = dict()

            def is_array_of_float(value: typing.Any) -> bool:
                if not isinstance(value, list):
                    return False
                if len(value) == 0:
                    return False
                for check in value:
                    if not isinstance(check, float):
                        return False
                return True

            for field, value in values.items():
                if isinstance(value, float):
                    simple[field] = value
                elif is_array_of_float(value):
                    arrays[field] = value
                else:
                    generic[field] = value

            message = {
                'type': 'data',
                'source': source,
            }

            if len(generic) > 0:
                message['values'] = sanitize_for_json(generic)

            def encode_float(v: float) -> bytes:
                if v is None or not isfinite(v):
                    return self._MVC_FLOAT
                try:
                    return struct.pack('<f', v)
                except OverflowError:
                    return self._MVC_FLOAT

            if len(simple) > 0:
                fields = list(simple.keys())
                raw = bytearray()
                for field in fields:
                    raw += encode_float(simple[field])
                message['simple'] = {
                    'fields': fields,
                    'values': b64encode(raw).decode('ascii'),
                }

            if len(arrays) > 0:
                fields = list(arrays.keys())
                encoded = []
                for field in fields:
                    raw = bytearray()
                    for v in arrays[field]:
                        raw += encode_float(v)
                    encoded.append(b64encode(raw).decode('ascii'))
                message['array'] = {
                    'fields': fields,
                    'contents': encoded,
                }

            await self.websocket.send_json(message)

        async def incoming_instrument_add(self, source: str,
                                          information: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
            await self.websocket.send_json({
                'type': 'instrument_add',
                'source': source,
                'info': information,
            })

        async def incoming_instrument_update(self, source: str, information: typing.Dict[str, typing.Any]) -> None:
            await self.websocket.send_json({
                'type': 'instrument_update',
                'source': source,
                'info': information,
            })

        async def incoming_instrument_remove(self, source: str) -> None:
            await self.websocket.send_json({
                'type': 'instrument_remove',
                'source': source,
            })

        async def incoming_instrument_state(self, source: str, state: typing.Dict[str, typing.Any]) -> None:
            await self.websocket.send_json({
                'type': 'instrument_state',
                'source': source,
                'state': state,
            })

        async def incoming_event_log(self, source: str, event: typing.Dict[str, typing.Any]) -> None:
            await self.websocket.send_json({
                'type': 'event_log',
                'source': source,
                'event': event,
            })

        async def incoming_chat(self, epoch_ms: int, name: str, text: str) -> None:
            await self.websocket.send_json({
                'type': 'chat',
                'from': name,
                'epoch_ms': epoch_ms,
                'message': text
            })

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.station: str = None
        self.origin: typing.Optional[str] = None
        self._client: typing.Optional[_AcquisitionSocket.Client] = None
        self._client_run: typing.Optional[asyncio.Task] = None

    def _is_writable(self, websocket: WebSocket):
        return websocket.user.allow_mode(self.station, 'acquisition', write=True)

    async def _execute_client(self):
        try:
            await self._client.run()
        except asyncio.CancelledError:
            raise
        finally:
            if self._client_run:
                try:
                    await self._client.websocket.close()
                except:
                    pass

    @requires('authenticated')
    async def on_connect(self, websocket: WebSocket):
        origin = websocket.headers.get('origin')
        if origin is not None and len(origin) > 0:
            origin = URL(url=origin)
            if origin.netloc != websocket.url.netloc:
                raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Cross origin disallowed")

        self.station = websocket.path_params['station'].lower()
        if self.station not in STATIONS:
            raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
        if not websocket.user.allow_mode(self.station, 'acquisition'):
            raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Station not available")

        try:
            self.origin = str(ipaddress.ip_address(websocket.client.host))
        except ValueError:
            self.origin = None

        try:
            client_reader, client_writer = await asyncio.open_unix_connection(
                CONFIGURATION.get('ACQUISITION.SOCKET', '/run/forge-vis-acquisition.socket'))
        except OSError:
            raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Acquisition dispatch unavailable")

        client = self.Client(client_reader, client_writer, websocket)

        try:
            await client.connect(self.station, True)
        except EOFError:
            raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Acquisition connection refused")

        await websocket.accept()

        self._client = client
        self._client_run = asyncio.ensure_future(self._execute_client())

    @requires('authenticated')
    async def on_receive(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]):
        if not self._is_writable(websocket):
            return

        message_type = data['type']
        if message_type == 'chat':
            text = str(data.get('message', '')).strip()
            if len(text) == 0:
                return

            author = str(data.get('from', '')).strip()
            if len(author) == 0:
                author = websocket.user.initials.upper()

            epoch_ms = data.get('epoch_ms', None)
            if epoch_ms:
                epoch_ms = int(epoch_ms)
            else:
                epoch_ms = round(time.time() * 1000)

            self._client.send_chat(epoch_ms, author, text)
            await self._client.writer.drain()

            # Echo it back to the sender for display
            await self._client.incoming_chat(epoch_ms, author, text)
        elif message_type == 'command':
            target = str(data.get('target', ''))
            command = str(data.get('command', ''))
            data = data.get('data', None)

            self._client.send_command(target, command, data)
            await self._client.writer.drain()
        elif message_type == 'write_message_log':
            text = str(data.get('text', '')).strip()
            if len(text) == 0:
                return

            author = str(data.get('author', '')).strip()
            if len(author) == 0:
                author = websocket.user.initials.upper()

            auxiliary = {
                'network_origin': self.origin,
                'user_id': websocket.user.display_id,
                'user_name': websocket.user.display_name,
            }

            self._client.send_message_log(author, text, auxiliary)
            await self._client.writer.drain()

            await websocket.send_json({
                'type': 'acknowledge_message_log',
                'result': 'ok',
            })
        elif message_type == 'set_bypass':
            bypassed = bool(data.get('bypassed', True))
            self._client.send_bypass(bypassed)
            await self._client.writer.drain()
        elif message_type == 'restart_acquisition':
            self._client.send_restart()
            await self._client.writer.drain()

    async def on_disconnect(self, websocket: WebSocket, close_code):
        if self._client_run:
            t = self._client_run
            self._client_run = None
            try:
                t.cancel()
            except:
                pass
            try:
                await t
            except:
                pass

        if self._client:
            self._client.writer.close()
            self._client = None

        await super().on_disconnect(websocket, close_code)


class _ExampleSocket(WebSocketEndpoint):
    encoding = 'json'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.station: str = None
        self.generate_tasks: typing.List[asyncio.Task] = list()
        self._spancheck_started = False

    @requires('authenticated')
    async def on_connect(self, websocket: WebSocket):
        origin = websocket.headers.get('origin')
        if origin is not None and len(origin) > 0:
            origin = URL(url=origin)
            if origin.netloc != websocket.url.netloc:
                raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Cross origin disallowed")

        self.station = websocket.path_params['station'].lower()
        if self.station not in STATIONS:
            raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
        if not websocket.user.allow_station(self.station):
            raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Station not available")

        await websocket.accept()

        await websocket.send_json({
            'type': 'instrument_add',
            'source': 'S11',
            'info': {
                'type': 'example_neph',
                'serial_number': 1234,
                'display_id': 'S11',
                'display_letter': 'N',
            },
        })
        await websocket.send_json({
            'type': 'instrument_state',
            'source': 'S11',
            'state': {
                'communicating': True,
            },
        })

        await websocket.send_json({
            'type': 'instrument_add',
            'source': 'X1',
            'info': {
                'type': 'example_none',
                'serial_number': 5678,
                'display_id': 'X1',
                'display_letter': 'X',
            },
        })
        await websocket.send_json({
            'type': 'instrument_state',
            'source': 'X1',
            'state': {
                'communicating': False,
            },
        })

        await websocket.send_json({
            'type': 'instrument_add',
            'source': 'X2',
            'info': {
                'type': 'example_none',
                'display_id': 'X2',
                'display_letter': 'Y',
            },
        })
        await websocket.send_json({
            'type': 'instrument_state',
            'source': 'X2',
            'state': {
                'communicating': True,
                'bypassed': True,
            },
        })

        self.generate_tasks.append(asyncio.ensure_future(self._generate_data(websocket)))
        self.generate_tasks.append(asyncio.ensure_future(self._generate_events(websocket)))
        self.generate_tasks.append(asyncio.ensure_future(self._generate_chat(websocket)))

    @requires('authenticated')
    async def on_receive(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]):
        action = data['type']
        if action == 'write_message_log':
            await websocket.send_json({
                'type': 'acknowledge_message_log',
                'result': 'ok',
            })
        elif action == 'restart_acquisition':
            await websocket.close()
        elif action == 'command':
            command = data['command']
            if command == 'start_spancheck' and not self._spancheck_started:
                self._spancheck_started = True
                self.generate_tasks.append(asyncio.ensure_future(self._spancheck_sequence(websocket)))
        elif action == 'chat':
            await websocket.send_json({
                'type': 'chat',
                'from': data.get('from', 'NONE'),
                'message': data.get('message', 'NONE'),
            })

    async def on_disconnect(self, websocket: WebSocket, close_code):
        for t in self.generate_tasks:
            try:
                t.cancel()
            except:
                pass
            try:
                await t
            except:
                pass
        self.generate_tasks.clear()
        await super().on_disconnect(websocket, close_code)

    @staticmethod
    async def _generate_data(websocket: WebSocket) -> None:
        import random
        while True:
            await asyncio.sleep(1)
            try:
                await websocket.send_json({
                    'type': 'data',
                    'source': 'S11',
                    'simple': {
                        'fields': ['BsB', 'BsG', 'BsR', 'BbsB', 'BbsG', 'BbsR'],
                        'values': b64encode(
                            struct.pack('<ffffff', *[random.uniform(1, 10) for i in range(6)])
                        ).decode('ascii')
                    },
                    'values': {
                        'state': 'NBXX',
                    },
                })
            except:
                _LOGGER.debug("Error generating example data", exc_info=True)
                return

    @staticmethod
    async def _generate_events(websocket: WebSocket) -> None:
        try:
            for i in range(5):
                await asyncio.sleep(1)
                await websocket.send_json({
                    'type': 'event_log',
                    'source': 'S11',
                    'event': {
                        'epoch_ms': (int(time.time() / 60) * 60 - 300) * 1000,
                        'message': "Long message " * 20,
                        'level': 'info',
                    }
                })

            await websocket.send_json({
                'type': 'event_log',
                'source': 'X1',
                'event': {
                    'message': "Example error",
                    'level': 'error',
                }
            })
        except:
            _LOGGER.debug("Error generating example events", exc_info=True)

    @staticmethod
    async def _generate_chat(websocket: WebSocket) -> None:
        import time

        try:
            for i in range(5):
                await asyncio.sleep(1)
                await websocket.send_json({
                    'type': 'chat',
                    'from': 'NONE',
                    'epoch_ms': int(time.time() * 60 * 1000),
                    'message': "Example message"
                })
        except:
            _LOGGER.debug("Error generating example chat", exc_info=True)

    @staticmethod
    async def _spancheck_sequence(websocket: WebSocket) -> None:
        import time

        async def send_spancheck_state(state: str, delay: float) -> None:
            await websocket.send_json({
                'type': 'data',
                'source': '_spancheck',
                'values': {
                    'state': {
                        'id': state,
                        'next_epoch_ms': int((time.time() + delay) * 1000),
                    },
                },
            })
            await asyncio.sleep(delay)

        try:
            await send_spancheck_state('gas_air_flush', 30.0)
            await send_spancheck_state('gas_flush', 60.0)
            await send_spancheck_state('gas_sample', 60.0)
            await send_spancheck_state('air_flush', 30.0)
            await send_spancheck_state('air_sample', 60.0)

            await websocket.send_json({
                'type': 'data',
                'source': '_spancheck',
                'values': {
                    'state': {
                        'id': 'inactive',
                    },
                    'percent_error': {
                        'S11': 1.25,
                    },
                },
            })
        except:
            _LOGGER.debug("Error generating example spancheck", exc_info=True)


sockets: typing.List[BaseRoute] = [
    WebSocketRoute('/{station}', _AcquisitionSocket, name='acquisition_socket'),
    WebSocketRoute('/{station}/example', _ExampleSocket, name='acquisition_example_socket'),
]

routes: typing.List[Route] = [
    Route('/{station}/display/{type}', endpoint=display, name='acquisition_display'),
    Route('/{station}/summary/{type}', endpoint=summary, name='acquisition_summary'),
]
