import asyncio
import typing
import struct
import os
import starlette.status
from base64 import b64encode
from starlette.routing import Route, BaseRoute, WebSocketRoute
from starlette.authentication import requires
from starlette.datastructures import URL
from starlette.responses import Response, HTMLResponse, StreamingResponse
from starlette.requests import Request
from starlette.endpoints import WebSocketEndpoint
from starlette.exceptions import HTTPException
from starlette.websockets import WebSocket
from forge.const import STATIONS
from forge.vis import CONFIGURATION
from forge.vis.util import package_template
from .permissions import is_available
from .assemble import visible_exports
from .controller.manager import Manager, ExportedFile


_manager: typing.Optional[Manager] = None


async def _export_connection(station: str, mode_name: str, export_key: str, start_epoch_ms: int, end_epoch_ms: int,
                             command: int = 0) -> typing.Tuple[typing.Optional[asyncio.StreamReader],
                                                               typing.Optional[asyncio.StreamWriter]]:
    socket_path = CONFIGURATION.get('EXPORT.SOCKET', None)
    if not socket_path:
        return None, None

    try:
        reader, writer = await asyncio.open_unix_connection(socket_path)

        header = bytes()

        def header_string(add: str) -> None:
            nonlocal header
            raw = add.encode('utf-8')
            header += struct.pack('<I', len(raw))
            header += raw

        header_string(station)
        header_string(mode_name)
        header_string(export_key)
        header += struct.pack('<q', start_epoch_ms)
        header += struct.pack('<q', end_epoch_ms)
        header += struct.pack('<B', command)
        writer.write(header)
        await writer.drain()

        return reader, writer
    except (OSError, EOFError):
        return None, None


class _ExportStream:
    def __init__(self, reader: typing.Union[asyncio.Future, asyncio.StreamReader],
                 writer: typing.Optional[asyncio.StreamWriter] = None):
        self.task: typing.Optional[asyncio.Task] = None

        self.size: typing.Optional[int] = None
        self.client_name: typing.Optional[str] = None
        self.media_type: typing.Optional[str] = None
        self.stream: typing.Optional[typing.AsyncGenerator] = None
        self.file: typing.Optional[ExportedFile] = None

        self._reader = reader
        self._writer = writer

    async def acquire(self) -> None:
        if not isinstance(self._reader, asyncio.StreamReader):
            try:
                self.file = await self._reader
            except asyncio.CancelledError:
                try:
                    self._reader.cancel()
                except:
                    pass
                raise
            self.size = self.file.size
            self.client_name = self.file.client_name
            self.media_type = self.file.media_type
            self.stream = self._file_stream()
            return

        async def string_arg() -> str:
            arg_len = struct.unpack('<I', await self._reader.readexactly(4))[0]
            return (await self._reader.readexactly(arg_len)).decode('utf-8')

        try:
            self.size = struct.unpack('<Q', await self._reader.readexactly(8))[0]
            self.client_name = await string_arg()
            self.media_type = await string_arg()
            self.stream = self._connection_stream()
        except asyncio.CancelledError:
            try:
                if self._writer:
                    self._writer.close()
            except:
                pass
            raise

    async def _file_stream(self) -> None:
        try:
            source = os.dup(self.file.file.fileno())
        except OSError:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Error initializing file")
        try:
            offset = 0
            while True:
                data = os.pread(source, 4096, offset)
                if not data:
                    break
                offset += len(data)
                yield data
        finally:
            try:
                os.close(source)
            except OSError:
                pass

    async def _connection_stream(self) -> None:
        try:
            while True:
                data = await self._reader.read(4096)
                if not data:
                    break
                yield data
        except asyncio.CancelledError:
            try:
                if self._writer:
                    self._writer.close()
            except:
                pass
            raise
        finally:
            try:
                if self._writer:
                    self._writer.close()
            except:
                pass


async def _export_stream(station: str, mode_name: str, export_key: str,
                         start_epoch_ms: int, end_epoch_ms: int, command: int = 0) -> _ExportStream:
    reader, writer = await _export_connection(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, command)
    if not reader:
        global _manager
        if not _manager:
            _manager = Manager()
        export_file = _manager(station, mode_name, export_key, start_epoch_ms, end_epoch_ms)
        if not export_file:
            raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid export")
        return _ExportStream(export_file)

    return _ExportStream(reader, writer)


@requires('authenticated')
async def _export_data(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    mode_name = request.path_params['mode_name'].lower()
    if not is_available(request.user, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Data not available")

    try:
        data = await request.json()
    except:
        data = {}

    export_key = request.query_params.get('key')
    if not export_key:
        export_key = data.get('key')
    if not export_key:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid export key")

    try:
        start_epoch_ms = request.query_params.get('start')
        if not start_epoch_ms:
            start_epoch_ms = data.get('start')
        start_epoch_ms = int(start_epoch_ms)
    except (ValueError, TypeError):
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid start time")
    try:
        end_epoch_ms = request.query_params.get('end')
        if not end_epoch_ms:
            end_epoch_ms = data.get('end')
        end_epoch_ms = int(end_epoch_ms)
    except (ValueError, TypeError):
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid end time")
    if start_epoch_ms <= 0 or end_epoch_ms <= 0 or end_epoch_ms <= start_epoch_ms:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid time bounds")

    source = await _export_stream(station, mode_name, export_key, start_epoch_ms, end_epoch_ms)
    await source.acquire()
    return StreamingResponse(source.stream, media_type=source.media_type, headers={
        'Content-Disposition': f'attachment; filename="{source.client_name}"',
        'Content-Length': f'{source.size}',
    })


@requires('authenticated')
async def _export_modal(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    mode_name = request.path_params['mode_name'].lower()
    if not is_available(request.user, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Data not available")

    exports = await visible_exports(station, mode_name)
    if not exports:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="No exports available")

    return HTMLResponse(await package_template('export', 'modal.html').render_async(
        request=request,
        station=station,
        mode_name=mode_name,
        available=exports,
    ))


class _ExportSocket(WebSocketEndpoint):
    encoding = 'json'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.station: str = None
        self._active: typing.Set[_ExportStream] = set()

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

    @requires('authenticated')
    async def on_receive(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]):
        action = data['action']
        if action == 'wait':
            mode_name = str(data['mode'])
            export_key = str(data['key'])
            start_epoch_ms = int(data['start_epoch_ms'])
            end_epoch_ms = int(data['end_epoch_ms'])
            stream_id = data.get('stream')

            if not is_available(websocket.user, self.station, mode_name):
                await websocket.send_json({
                    'stream': stream_id,
                    'type': 'error',
                    'error': 'Access denied'
                })
                return

            source = await _export_stream(self.station, mode_name, export_key, start_epoch_ms, end_epoch_ms, 1)
            if not source:
                await websocket.send_json({
                    'stream': stream_id,
                    'type': 'error',
                    'error': 'No data available'
                })
                return

            async def wait_ready():
                try:
                    await source.acquire()

                    await websocket.send_json({
                        'stream': stream_id,
                        'type': 'ready',
                        'station': self.station,
                        'key': export_key,
                        'start_epoch_ms': start_epoch_ms,
                        'end_epoch_ms': end_epoch_ms,
                        'size': source.size,
                        'filename': source.client_name,
                        'media_type': source.media_type,
                    })
                except asyncio.CancelledError:
                    pass

                self._active.discard(source)

            source.task = asyncio.ensure_future(wait_ready())
            self._active.add(source)
        elif action == 'stream':
            mode_name = str(data['mode'])
            export_key = str(data['key'])
            start_epoch_ms = int(data['start_epoch_ms'])
            end_epoch_ms = int(data['end_epoch_ms'])
            stream_id = data.get('stream')

            if not is_available(websocket.user, self.station, mode_name):
                await websocket.send_json({
                    'stream': stream_id,
                    'type': 'end',
                    'error': 'Access denied'
                })
                return

            source = await _export_stream(self.station, mode_name, export_key, start_epoch_ms, end_epoch_ms)
            if not source:
                await websocket.send_json({
                    'stream': stream_id,
                    'type': 'end',
                    'error': "No data available"
                })
                return

            async def stream_data():
                try:
                    await source.acquire()

                    await websocket.send_json({
                        'stream': stream_id,
                        'type': 'begin',
                        'station': self.station,
                        'key': export_key,
                        'start_epoch_ms': start_epoch_ms,
                        'end_epoch_ms': end_epoch_ms,
                        'size': source.size,
                        'filename': source.client_name,
                        'media_type': source.media_type,
                    })

                    async for chunk in source.stream:
                        await websocket.send_json({
                            'stream': stream_id,
                            'type': 'data',
                            'data': b64encode(chunk)
                        })

                    await websocket.send_json({
                        'stream': stream_id,
                        'type': 'end',
                    })
                except asyncio.CancelledError:
                    pass

                self._active.discard(source)

            source.task = asyncio.ensure_future(stream_data())
            self._active.add(source)
        else:
            await websocket.send_json({'type': 'error', 'error': "Invalid request"})

    async def on_disconnect(self, websocket, close_code):
        for stream in list(self._active):
            try:
                if stream.task:
                    stream.task.cancel()
                stream.task = None
            except:
                pass


routes: typing.List[Route] = [
    Route('/{station}/{mode_name}/modal', endpoint=_export_modal, name='export_modal'),
    Route('/{station}/{mode_name}', endpoint=_export_data, methods=['GET', 'POST'], name='export_data'),
]

sockets: typing.List[BaseRoute] = [
    WebSocketRoute('/{station}', _ExportSocket, name='export_socket'),
]
