import typing
import os
import asyncio
import logging
import argparse
import signal
import struct
import aiohttp
from base64 import b64decode
from os.path import exists as file_exists
from dynaconf import Dynaconf
from dynaconf.constants import DEFAULT_SETTINGS_FILES
from starlette.datastructures import URL
from forge.tasks import background_task
from forge.authsocket import WebsocketBinary as AuthSocket, PrivateKey, key_to_bytes
from .protocol import ToRemotePacketType, FromRemotePacketType

CONFIGURATION = Dynaconf(
    environments=False,
    lowercase_read=False,
    merge_enabled=True,
    default_settings_paths=DEFAULT_SETTINGS_FILES,
)
_LOGGER = logging.getLogger(__name__)


class UplinkConnection:
    class _SSHConnection:
        def __init__(self, connection_id: int, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                     uplink_data: typing.Callable[[bytes], typing.Awaitable[None]]):
            self.connection_id: typing.Optional[int] = connection_id
            self.reader = reader
            self.writer = writer
            self._uplink_data = uplink_data
            self._incoming_task: typing.Optional[asyncio.Task] = None

        async def send(self, data: bytes) -> None:
            if self.writer:
                self.writer.write(data)
                await self.writer.drain()

        async def _dispatch_incoming(self) -> None:
            while True:
                data = await self.reader.read(0xFFFF)
                if not data:
                    return
                if self.connection_id is None:
                    return
                await self._uplink_data(data)

        async def run(self):
            self._incoming_task = asyncio.ensure_future(self._dispatch_incoming())
            try:
                await self._incoming_task
            except asyncio.CancelledError:
                pass

        def closed(self) -> None:
            if self.writer:
                try:
                    self.writer.close()
                except OSError:
                    pass
                self.writer = None

        def disconnect(self) -> None:
            if self._incoming_task:
                self._incoming_task.cancel()
                self._incoming_task = None
            self.closed()

    def __init__(self, key: PrivateKey, url: URL, ssh_port: int):
        self.key = key
        self.url = url
        self.ssh_port = ssh_port
        self.websocket: "aiohttp.client.ClientWebSocketResponse" = None
        self._connections: typing.Dict[int, UplinkConnection._SSHConnection] = dict()

    async def _run_ssh_connection(self, connection_id: int):
        try:
            reader, writer = await asyncio.open_connection(host='localhost', port=self.ssh_port)
        except:
            _LOGGER.warning("SSH loopback connection failed", exc_info=True)
            try:
                await self.websocket.send_bytes(struct.pack('<BH', FromRemotePacketType.CONNECTION_CLOSED.value,
                                                            connection_id))
            except ConnectionError:
                pass
            return

        async def _send(data: bytes):
            await self.websocket.send_bytes(struct.pack('<BH', FromRemotePacketType.DATA.value, connection_id) + data)

        connection = self._SSHConnection(connection_id, reader, writer, _send)
        self._connections[connection_id] = connection
        try:
            await self.websocket.send_bytes(struct.pack('<BH', FromRemotePacketType.CONNECTION_OPEN.value,
                                                        connection_id))
            await connection.run()
        except OSError:
            # This could be a read error or a write to a closed websocket
            _LOGGER.debug("Error in SSH connection dispatch", exc_info=True)
        finally:
            if connection.connection_id is not None:
                self._connections.pop(connection.connection_id, None)
                closed_id = connection.connection_id
                connection.connection_id = None
                connection.closed()
                try:
                    await self.websocket.send_bytes(struct.pack('<BH', FromRemotePacketType.CONNECTION_CLOSED.value,
                                                                closed_id))
                except ConnectionError:
                    pass

    async def _dispatch_packet(self, data: bytes) -> None:
        packet_type = ToRemotePacketType(struct.unpack('<B', data[:1])[0])
        if packet_type == ToRemotePacketType.DATA:
            connection_id = struct.unpack('<H', data[1:3])[0]
            target = self._connections.get(connection_id)
            if target:
                try:
                    await target.send(data[3:])
                except OSError:
                    target.disconnect()
            # Not found is ok (closed locally, but server hasn't received that yet)
        elif packet_type == ToRemotePacketType.SSH_CONNECTION_OPEN:
            connection_id = struct.unpack('<H', data[1:3])[0]
            prior = self._connections.pop(connection_id, None)
            if prior:
                _LOGGER.debug(f"Closing duplicate connection {connection_id}")
                prior.connection_id = None
                prior.disconnect()

            _LOGGER.debug(f"Opening SSH connection {connection_id}")
            background_task(self._run_ssh_connection(connection_id))
        elif packet_type == ToRemotePacketType.CONNECTION_CLOSE:
            connection_id = struct.unpack('<H', data[1:3])[0]
            target = self._connections.pop(connection_id, None)
            if target:
                target.connection_id = None
                _LOGGER.debug(f"Disconnecting connection {connection_id}")
                target.disconnect()
            # Not found is ok (closed locally, but server hasn't received that yet)
        else:
            raise ValueError("Invalid packet type")

    async def run(self):
        timeout = aiohttp.ClientTimeout(connect=30, sock_read=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(str(self.url)) as websocket:
                self.websocket = websocket
                await AuthSocket.client_handshake(self.websocket, self.key)
                _LOGGER.info(f"Tunnel connected to {self.url}")

                async for msg in self.websocket:
                    if msg.type == aiohttp.WSMsgType.BINARY:
                        await self._dispatch_packet(msg.data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        _LOGGER.debug(f"Websocket error {msg}")
                        return

    def disconnect(self):
        for connection in self._connections.values():
            connection.disconnect()


async def run(key: PrivateKey, url: URL, ssh_port: int):
    while True:
        uplink = UplinkConnection(key, url, ssh_port)
        try:
            try:
                await uplink.run()
            except:
                _LOGGER.info(f"Connection to {url} terminated", exc_info=True)
        finally:
            try:
                uplink.disconnect()
            except:
                _LOGGER.debug(f"Uplink disconnection error", exc_info=True)
        await asyncio.sleep(60)


def main():
    parser = argparse.ArgumentParser(description="Tunnel remote endpoint.")

    default_url = CONFIGURATION.get('TUNNEL.SOCKET')
    if not default_url:
        default_url = CONFIGURATION.get('TELEMETRY.URL')
        if default_url:
            if isinstance(default_url, str):
                default_url = [default_url]
            elif not isinstance(default_url, list):
                default_url = None
            if default_url:
                for url in default_url:
                    url = URL(url=url)

                    if url.scheme == 'wss':
                        # Path will be something like: '/socket/update', and we need '/socket/ssh/open'
                        url = url.replace(spath=url.path[:-6] + 'ssh/open')
                    elif url.scheme == 'ws':
                        url = url.replace(path=url.path[:-6] + 'ssh/open')
                    elif url.scheme == 'http' or url.scheme == 'https':
                        # Path will be something like: '/update', and we need '/socket/ssh/open'
                        url = url.replace(path=url.path[:-6] + 'socket/ssh/open')
                    else:
                        continue
                    default_url = str(url)
                    break
            if not isinstance(default_url, str):
                default_url = None

    parser.add_argument('url',
                        help="telemetry server websocket URL",
                        default=default_url,
                        nargs=default_url and '?' or 1)

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--systemd',
                        dest='systemd', action='store_true',
                        help="enable systemd service integration")

    parser.add_argument('--ssh-port',
                        dest='ssh_port', type=int, default=22,
                        help="local SSH port")
    parser.add_argument('--key',
                        dest='key',
                        help="system key file")

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
    if url.scheme == 'wss':
        url = url.replace(scheme='https')
    else:
        url = url.replace(scheme='http')

    loop = asyncio.get_event_loop()

    if args.systemd:
        import systemd.daemon

        async def heartbeat():
            systemd.daemon.notify("READY=1")
            while True:
                await asyncio.sleep(10)
                systemd.daemon.notify("WATCHDOG=1")

        background_task(heartbeat())

    background_task(run(key, url, args.ssh_port))
    loop.add_signal_handler(signal.SIGINT, loop.stop)
    loop.add_signal_handler(signal.SIGTERM, loop.stop)
    loop.run_forever()


if __name__ == '__main__':
    main()