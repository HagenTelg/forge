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
from .stream import DataStream
from .assemble import begin_stream


_LOGGER = logging.getLogger(__name__)


class _DataSocket(WebSocketEndpoint):
    encoding = 'json'

    class _ActiveStream:
        def __init__(self, stream: DataStream):
            self.stream = stream
            self.stopped = False
            self.task: asyncio.Task = None
            self.stall: typing.Optional[DataStream.Stall] = None
            self.stall_task: typing.Optional[asyncio.Task] = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.station: str = None
        self.active_data_streams: typing.Dict[int, _DataSocket._ActiveStream] = dict()
        self.was_data_stalled = False
        self.prior_stall_reason: typing.Optional[str] = None

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

    async def _update_stall_state(self, websocket: WebSocket):
        is_stalled = False
        stall_reason: typing.Optional[str] = None
        for stream in self.active_data_streams.values():
            if not stream.stall:
                continue
            is_stalled = True
            if not stall_reason:
                stall_reason = stream.stall.reason
        if is_stalled and (not self.was_data_stalled or self.prior_stall_reason != stall_reason):
            self.was_data_stalled = True
            self.prior_stall_reason = stall_reason
            await websocket.send_json({
                'type': 'stalled',
                'stalled': True,
                'reason': stall_reason,
            })
        elif not is_stalled and self.was_data_stalled:
            self.was_data_stalled = False
            self.prior_stall_reason = None
            await websocket.send_json({
                'type': 'stalled',
                'stalled': False,
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

            stream = begin_stream(websocket.user, self.station, data_name, start_epoch_ms, end_epoch_ms, send)
            if stream is None:
                _LOGGER.debug(f"No data stream available for {data_name} to {websocket.client.host}")
                await self._end_data_stream(websocket, stream_id)
                return

            if stream_id in self.active_data_streams:
                try:
                    self.active_data_streams[stream_id].task.cancel()
                except:
                    pass

            active_stream = self._ActiveStream(stream)
            self.active_data_streams[stream_id] = active_stream

            async def run_stream():
                try:
                    stall = await stream.stall()
                    if stall:
                        active_stream.stall = stall

                        async def wait_stall():
                            await stall.block()

                        stall_task = asyncio.ensure_future(wait_stall())
                        active_stream.stall_task = stall_task

                        _LOGGER.debug(f"Stalling stream {stream_id} to {websocket.client.host}")
                        await self._update_stall_state(websocket)

                        try:
                            await stall_task
                        except asyncio.CancelledError:
                            pass

                        active_stream.stall_task = None
                        active_stream.stall = None
                        await self._update_stall_state(websocket)
                        _LOGGER.debug(f"Stall completed for {stream_id} to {websocket.client.host}")

                    if active_stream.stopped:
                        return
                    await stream.run()
                except asyncio.CancelledError:
                    return
                if active_stream.stopped:
                    return

                _LOGGER.debug(f"Completed data stream {stream_id} to {websocket.client.host}")
                await self._end_data_stream(websocket, stream_id)
                try:
                    del self.active_data_streams[stream_id]
                except KeyError:
                    pass

            _LOGGER.debug(f"Starting data stream {stream_id} for {data_name} to {websocket.client.host}")

            active_stream.task = asyncio.ensure_future(run_stream())

        elif action == 'unstall':
            for stream in self.active_data_streams.values():
                if not stream.stall_task:
                    continue
                try:
                    stream.stall_task.cancel()
                except:
                    pass
            await self._update_stall_state(websocket)

        elif action == 'stop':
            stream_id = int(data['stream'])
            stream = self.active_data_streams.get(stream_id)
            if stream_id is None:
                return
            del self.active_data_streams[stream_id]
            stream.stopped = True

            if stream.stall_task:
                try:
                    stream.stall_task.cancel()
                except:
                    pass

            stream.task.cancel()
            try:
                await stream.task
            except asyncio.CancelledError:
                await self._end_data_stream(websocket, stream_id)

            await self._update_stall_state(websocket)

            _LOGGER.debug(f"Aborted data stream {stream_id} to {websocket.client.host}")

        else:
            await websocket.send_json({'type': 'error', 'error': "Invalid request"})

    async def on_disconnect(self, websocket, close_code):
        for stream in list(self.active_data_streams.values()):
            stream.stopped = True
            try:
                stream.task.cancel()
            except:
                pass


sockets: typing.List[BaseRoute] = [
    WebSocketRoute('/{station}', _DataSocket, name='data_socket'),
]
