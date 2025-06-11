import typing
import asyncio
import logging
from starlette.websockets import WebSocket
from forge.const import STATIONS
from forge.vis.realtime.translation import get_translator as get_realtime_translator, RealtimeTranslator
from forge.vis.realtime.controller.client import WriteData as RealtimeOutput
from forge.vis.acquisition.translation import get_translator as get_acquisition_translator, AcquisitionTranslator
from .. import CONFIGURATION
from .socket import BusSocket
from ..bus import BusInterface as BusInterface, PersistenceLevel
from ..realtime import RealtimeTranslatorOutput
from ..acquisition import AcquisitionTranslatorClient


_LOGGER = logging.getLogger(__name__)


class DownlinkSocket(BusSocket, BusInterface):
    def __init__(self, *args, **kwargs):
        BusSocket.__init__(self, *args, **kwargs)

        self.station: str = None
        self.realtime_output: typing.Optional[RealtimeTranslatorOutput] = None
        self.acquisition_client: typing.Optional[AcquisitionTranslatorClient] = None

    async def _check_connection_allowed(self, websocket: WebSocket) -> bool:
        if CONFIGURATION.get('ACQUISITION.DISABLE_ACCESS_CONTROL', False):
            return True
        return await websocket.scope['processing'].acquisition_uplink_authorized(self.public_key, self.station)

    async def handshake(self, websocket: WebSocket, data: bytes) -> bool:
        self.station = websocket.path_params['station'].lower()
        if self.station not in STATIONS:
            _LOGGER.debug(f"Rejecting invalid station {self.station} for {self.display_id}")
            return False

        self.display_id = f"{self.display_id} - {self.station.upper()}"
        if not await self._check_connection_allowed(websocket):
            _LOGGER.debug(f"Rejected acquisition uplink for {self.display_id}")
            return False

        if not await super().handshake(websocket, data):
            return False

        translator = get_realtime_translator(self.station)
        if translator and isinstance(translator, RealtimeTranslator):
            realtime_socket_name = CONFIGURATION.get('REALTIME.SOCKET', '/run/forge-vis-realtime.socket')
            _LOGGER.debug(f"Connecting realtime translator for {self.display_id} to {realtime_socket_name}")
            try:
                reader, writer = await asyncio.open_unix_connection(realtime_socket_name)
                output = RealtimeOutput(reader, writer)
                await output.connect()
                realtime_output = RealtimeTranslatorOutput(self.station, output, translator)

                self.realtime_output = realtime_output
            except OSError:
                _LOGGER.warning(f"Failed to connect realtime data socket {realtime_socket_name}", exc_info=True)

        translator = get_acquisition_translator(self.station)
        if translator and isinstance(translator, AcquisitionTranslator):
            acquisition_socket_name = CONFIGURATION.get('ACQUISITION.SOCKET', '/run/forge-vis-acquisition.socket')
            _LOGGER.debug(f"Connecting acquisition translator for {self.display_id} to {acquisition_socket_name}")
            try:
                reader, writer = await asyncio.open_unix_connection(acquisition_socket_name)
                acquisition_client = AcquisitionTranslatorClient(reader, writer, translator,
                                                                 self.has_instantaneous_data)
                acquisition_client.data_consolidation_time = 0.5
                await acquisition_client.connect(self.station, False)

                self.acquisition_client = acquisition_client
            except OSError:
                _LOGGER.warning(f"Failed to connect acquisition controller socket {acquisition_socket_name}",
                                exc_info=True)

        if self.realtime_output:
            await self.realtime_output.start()
        if self.acquisition_client:
            self.acquisition_client.bus = self
            await self.acquisition_client.start()

        _LOGGER.debug(f"Acquisition uplink connected for {self.display_id}")
        return True

    async def on_disconnect(self, websocket: WebSocket, close_code):
        await super().on_disconnect(websocket, close_code)

        if self.realtime_output:
            c = self.realtime_output
            self.realtime_output = None
            await c.shutdown()

        if self.acquisition_client:
            c = self.acquisition_client
            self.acquisition_client = None
            await c.shutdown()

    async def incoming_message(self, source: str, record: str, message: typing.Any) -> None:
        if self.acquisition_client:
            self.acquisition_client.incoming_message(source, record, message)
        if self.realtime_output:
            self.realtime_output.incoming_message(source, record, message)

    async def send_message(self, level: PersistenceLevel, record: str, message: typing.Any) -> None:
        await BusSocket.send_message(self, level, record, message)
