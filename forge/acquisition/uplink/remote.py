import typing
import asyncio
import logging
import argparse
import signal
import aiohttp
import struct
import io
from collections import OrderedDict
from base64 import b64decode
from os.path import exists as file_exists
from starlette.datastructures import URL
from forge.acquisition.bus.client import AcquisitionBusClient
from forge.authsocket import WebsocketBinary as AuthSocket, PrivateKey
from . import CONFIGURATION
from .bus import PersistenceLevel
from .incoming.protocol import PROTOCOL_VERSION, UplinkSerializer


_LOGGER = logging.getLogger(__name__)


class LocalBusClient(AcquisitionBusClient):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, uplink: "UplinkConnection"):
        super().__init__("_UPLINK", reader, writer, disable_echo=True)
        self.uplink = uplink

    async def incoming_message(self, source: str, record: str, message: typing.Any) -> None:
        self.uplink.incoming_message(source, record, message)


class SourceKey:
    def __init__(self, source: str, record: str):
        self.source = source
        self.record = record

    def __eq__(self, other):
        if not isinstance(other, SourceKey):
            return NotImplemented
        return self.source == other.source and self.record == other.record

    def __hash__(self):
        return hash((self.source, self.record))


class UplinkConnection:
    QUEUE_DISCARD_LIMIT = 16

    def __init__(self, key: PrivateKey, url: URL, args: argparse.Namespace):
        self.key = key
        self.url = url
        self.args = args
        self.include_instantaneous = args.include_instantaneous
        self.websocket: "aiohttp.client.ClientWebSocketResponse" = None
        self.bus: LocalBusClient = None
        self.bus_serializer = UplinkSerializer()

        self._flush_task: typing.Optional[asyncio.Task] = None
        self._queued_messages: typing.Dict[SourceKey, typing.List[typing.Any]] = OrderedDict()
        self._squashed_data: typing.Dict[SourceKey, typing.Dict[str, typing.Any]] = dict()

    async def _websocket_packet(self, data: bytes) -> None:
        reader = io.BytesIO(data)
        level = PersistenceLevel(struct.unpack('<B', reader.read(1))[0])
        record = await self.bus_serializer.deserialize_string_lookup(reader)
        message = await self.bus_serializer.deserialize_message(reader)
        self.bus.send_message(level, record, message)

    async def _read_websocket(self) -> None:
        async for msg in self.websocket:
            if msg.type == aiohttp.WSMsgType.BINARY:
                await self._websocket_packet(msg.data)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                return

    async def _send_message(self, source: str, record: str, message: typing.Any) -> None:
        if not self.websocket:
            return
        data = io.BytesIO()
        self.bus_serializer.serialize_string_lookup(data, source)
        self.bus_serializer.serialize_string_lookup(data, record)
        self.bus_serializer.serialize_message(data, message)
        await self.websocket.send_bytes(bytes(data.getbuffer()))
        data.close()

    async def _flush_queued_mesages(self) -> None:
        await asyncio.sleep(1.0)

        messages = list(self._queued_messages.items())
        data = list(self._squashed_data.items())
        self._queued_messages.clear()
        self._squashed_data.clear()
        self._flush_task = None

        for key, queued in messages:
            for message in queued:
                await self._send_message(key.source, key.record, message)
        for key, message in data:
            await self._send_message(key.source, key.record, message)

    def _include_message(self, record: str) -> bool:
        if self.include_instantaneous:
            return True
        if record == 'data':
            return False
        return True

    @staticmethod
    def _should_squash_data(record: str) -> bool:
        if record == 'data':
            return True
        if record == 'avg' or record.startswith('avg.'):
            return True
        return False

    def _insert_squashed_data(self, key: SourceKey, message: typing.Any) -> bool:
        existing = self._squashed_data.get(key)
        if existing is None:
            if message is None:
                return False
            self._squashed_data[key] = message
            return True
        elif not isinstance(existing, dict) or not isinstance(message, dict):
            self._squashed_data[key] = message
            return True

        for name, value in message.items():
            existing[name] = value
        return True

    @staticmethod
    def _should_throttle_data(record: str) -> bool:
        if record == 'chat':
            return False
        return True

    def incoming_message(self, source: str, record: str, message: typing.Any) -> None:
        if not self._include_message(record):
            return

        key = SourceKey(source, record)

        if self._should_squash_data(record):
            if not self._insert_squashed_data(key, message):
                return
        else:
            existing = self._queued_messages.get(key)
            if existing is None:
                existing = list()
                self._queued_messages[key] = existing

            if message is None:
                existing.clear()
                existing.append(message)
            else:
                if self._should_throttle_data(record):
                    n_discard = len(existing) - self.QUEUE_DISCARD_LIMIT
                    if n_discard > 0:
                        del existing[:n_discard]
                existing.append(message)

        if self._flush_task:
            return
        self._flush_task = asyncio.get_event_loop().create_task(self._flush_queued_mesages())

    async def run(self):
        websocket_task: typing.Optional[asyncio.Task] = None
        bus_wait: typing.Optional[asyncio.Task] = None
        try:
            timeout = aiohttp.ClientTimeout(connect=30, sock_read=60)
            _LOGGER.debug(f"Starting connection to {self.url}")
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.ws_connect(str(self.url)) as websocket:
                    await AuthSocket.client_handshake(websocket, self.key,
                                                      extra_data=struct.pack('<BB', PROTOCOL_VERSION,
                                                                             self.include_instantaneous and 1 or 0))
                    self.websocket = websocket
                    _LOGGER.debug(f"Websocket connected to {self.url}")

                    _LOGGER.debug("Connecting to acquisition bus")
                    reader, writer = await asyncio.open_unix_connection(self.args.bus_socket)
                    bus_client = LocalBusClient(reader, writer, self)
                    await bus_client.start()
                    self.bus = bus_client

                    bus_wait = asyncio.ensure_future(bus_client.wait())
                    websocket_task = asyncio.ensure_future(self._read_websocket())
                    await asyncio.wait([bus_wait, websocket_task], return_when=asyncio.FIRST_COMPLETED)
        finally:
            self.websocket = None
            bus = self.bus
            self.bus = None

            if websocket_task:
                _LOGGER.debug("Shutting down websocket")
                try:
                    websocket_task.cancel()
                except:
                    pass
                try:
                    await websocket_task
                except:
                    pass

            flush_task = self._flush_task
            self._flush_task = None
            if flush_task:
                try:
                    flush_task.cancel()
                except:
                    pass

            if bus_wait:
                try:
                    bus_wait.cancel()
                except:
                    pass
                try:
                    await bus_wait
                except:
                    pass

            if bus:
                _LOGGER.debug("Shutting down bus interface")
                try:
                    await bus.shutdown()
                except:
                    pass


def main():
    parser = argparse.ArgumentParser(description="Acquisition websocket remote uplink.")

    default_station = CONFIGURATION.get("ACQUISITION.STATION", 'nil').lower()
    default_url = CONFIGURATION.get('ACQUISITION.UPLINK_URL')
    if default_url:
        url = URL(url=default_url)
        if '{station}' in url.path:
            url = url.replace(path=url.path.replace('{station}', default_station))
        elif default_station and not url.path.endswith(f'/{default_station}'):
            url = url.replace(path=f'{url.path}/{default_station}')
        default_url = str(url)

    parser.add_argument('url',
                        help="Acquisition uplink websocket URL",
                        default=default_url,
                        nargs=default_url and '?' or 1)

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--systemd',
                        dest='systemd', action='store_true',
                        help="enable systemd service integration")
    parser.add_argument('--key',
                        dest='key',
                        help="system key file")

    parser.add_argument('--instantaneous',
                        dest='include_instantaneous', action='store_true',
                        help="include instantaneous data")
    parser.add_argument('--no-instantaneous',
                        dest='include_instantaneous', action='store_false',
                        help="do not include instantaneous data")
    parser.set_defaults(include_instantaneous=bool(CONFIGURATION.get('ACQUISITION.INCLUDE_INSTANTANEOUS', False)))

    parser.add_argument('--bus-socket',
                        dest='bus_socket', type=str,
                        default=CONFIGURATION.get('ACQUISITION.BUS', '/run/forge-acquisition-bus.socket'),
                        help="acquisition bus socket")

    parser.add_argument('--no-retry',
                        dest='no_retry', action='store_true',
                        help="disable automatic connection retry")


    args = parser.parse_args()
    if args.debug:
        root_logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)-40s %(message)s')
        handler.setFormatter(formatter)
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)

    key = args.key
    if key is None:
        key = CONFIGURATION.SYSTEM.KEY
    if file_exists(key):
        with open(key, 'rb') as f:
            key = f.read()
        if len(key) == 32:
            key = PrivateKey.from_private_bytes(key)
        else:
            key = PrivateKey.from_private_bytes(b64decode(key.decode('ascii').strip()))
    else:
        key = PrivateKey.from_private_bytes(b64decode(key))

    url = args.url
    if isinstance(url, list):
        url = url[0]
    url = URL(url=url)
    _LOGGER.info(f"Acquisition uplink to {url} starting")
    if url.scheme == 'wss':
        url = url.replace(scheme='https')
    elif url.scheme == 'ws':
        url = url.replace(scheme='http')

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    heartbeat: typing.Optional[asyncio.Task] = None
    if args.systemd:
        import systemd.daemon
        systemd.daemon.notify("READY=1")

        _LOGGER.debug("Starting systemd heartbeat")

        async def send_heartbeat() -> typing.NoReturn:
            while True:
                await asyncio.sleep(10)
                systemd.daemon.notify("WATCHDOG=1")

        heartbeat = loop.create_task(send_heartbeat())

    async def run():
        while True:
            uplink = UplinkConnection(key, url, args)
            try:
                await uplink.run()
            except asyncio.CancelledError:
                raise
            except:
                _LOGGER.info(f"Connection to {url} terminated", exc_info=True)
            if args.no_retry:
                return
            await asyncio.sleep(60)

    connection_run = loop.create_task(run())
    loop.add_signal_handler(signal.SIGINT, connection_run.cancel)
    loop.add_signal_handler(signal.SIGTERM, connection_run.cancel)
    try:
        loop.run_until_complete(connection_run)
    except asyncio.CancelledError:
        pass
    _LOGGER.debug("Remote visualization uplink stop")

    if heartbeat:
        _LOGGER.debug("Shutting down heartbeat")
        t = heartbeat
        heartbeat = None
        try:
            t.cancel()
        except:
            pass
        try:
            loop.run_until_complete(t)
        except:
            pass


if __name__ == '__main__':
    main()
