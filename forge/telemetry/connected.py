import typing
import asyncio
import logging
import ipaddress
import time
from starlette.websockets import WebSocket
from forge.authsocket import WebsocketJSON as AuthSocket
from .storage import Interface as TelemetryInterface

_LOGGER = logging.getLogger(__name__)


class TelemetrySocket(AuthSocket):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.telemetry: TelemetryInterface = None
        self.station: typing.Optional[str] = None
        self.origin: typing.Optional[str] = None
        self._ping_task: typing.Optional[asyncio.Task] = None

    async def handshake(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]) -> bool:
        self.telemetry = websocket.scope['telemetry']

        try:
            self.origin = str(ipaddress.ip_address(websocket.client.host))
        except ValueError:
            self.origin = None

        self.station = data.get('station')
        if self.station is not None:
            self.station = str(self.station).strip()
            self.station = self.station[:32].lower()
            if self.station == '*':
                return False
            self.display_id = f"{self.display_id} - {self.station.upper()}"

        await self.telemetry.ping_host(self.public_key, self.origin, self.station)
        self._ping_task = asyncio.ensure_future(self._ping())
        return True

    async def websocket_data(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]) -> None:
        request = data['request']
        if request == 'update':
            await self.telemetry.connected_update(self.public_key, self.origin, self.station,
                                                  data.get('telemetry', {}))
        elif request == 'partial':
            await self.telemetry.connected_update(self.public_key, self.origin, self.station,
                                                  data.get('telemetry', {}), partial=True)
        elif request == 'log':
            log_type = data['log']
            log_events = data['events']
            if log_type == 'kernel':
                await self.telemetry.append_log_kernel(self.public_key, self.origin, self.station,
                                                       log_events)
            elif log_type == 'acquisition':
                await self.telemetry.append_log_acquisition(self.public_key, self.origin, self.station,
                                                            log_events)
        elif request == 'get_time':
            await websocket.send_json({
                'response': 'server_time',
                'server_time': round(time.time()),
            })

    async def _ping(self):
        while True:
            await asyncio.sleep(60)
            await self.telemetry.ping_host(self.public_key, self.origin, self.station)
