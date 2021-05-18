import typing
import asyncio
import logging
import starlette.status
from starlette.routing import BaseRoute, WebSocketRoute
from starlette.datastructures import URL
from starlette.authentication import requires
from starlette.endpoints import WebSocketEndpoint
from starlette.exceptions import HTTPException
from starlette.websockets import WebSocket
from forge.const import STATIONS
from .assemble import begin_stream


_LOGGER = logging.getLogger(__name__)


class _DataSocket(WebSocketEndpoint):
    encoding = 'json'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.station: str = None
        self.active_data_streams: typing.Dict[int, asyncio.Task] = dict()

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

    @staticmethod
    async def _end_data_stream(websocket: WebSocket, stream_id: int):
        await websocket.send_json({
            'type': 'end',
            'stream': stream_id,
        })

    @requires('authenticated')
    async def on_receive(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]):
        action = data['action']
        if action == 'start':
            stream_id = int(data['stream'])
            data_name = str(data['data'])
            start_epoch_ms = int(data['start_epoch_ms'])
            end_epoch_ms = int(data['end_epoch_ms'])

            async def send(send_data: typing.Dict) -> None:
                await websocket.send_json({
                    'type': 'data',
                    'stream': stream_id,
                    'content': send_data,
                })

            stream = await begin_stream(websocket.user, self.station, data_name, start_epoch_ms, end_epoch_ms, send)
            if stream is None:
                _LOGGER.debug(f"No data stream available for {data_name} to {websocket.client.host}")
                await self._end_data_stream(websocket, stream_id)
                return

            if stream_id in self.active_data_streams:
                try:
                    self.active_data_streams[stream_id].cancel()
                except:
                    pass

            async def run_stream():
                try:
                    await stream.run()
                except asyncio.CancelledError:
                    return
                _LOGGER.debug(f"Completed data stream {stream_id} to {websocket.client.host}")
                await self._end_data_stream(websocket, stream_id)
                try:
                    del self.active_data_streams[stream_id]
                except KeyError:
                    pass

            _LOGGER.debug(f"Starting data stream {stream_id} for {data_name} to {websocket.client.host}")

            self.active_data_streams[stream_id] = asyncio.create_task(run_stream())

        elif action == 'stop':
            stream_id = int(data['stream'])
            stream = self.active_data_streams.get(stream_id)
            if stream_id is None:
                return
            del self.active_data_streams[stream_id]

            stream.cancel()
            try:
                await stream
            except asyncio.CancelledError:
                await self._end_data_stream(websocket, stream_id)

            _LOGGER.debug(f"Aborted data stream {stream_id} to {websocket.client.host}")

        else:
            await websocket.send_json({'type': 'error', 'error': "Invalid request"})

    async def on_disconnect(self, websocket, close_code):
        for task in list(self.active_data_streams.values()):
            try:
                task.cancel()
            except:
                pass


sockets: typing.List[BaseRoute] = [
    WebSocketRoute('/{station}', _DataSocket, name='data_socket'),
]
