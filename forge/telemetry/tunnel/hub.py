import typing
import asyncio
import logging
import struct
from base64 import b64encode
from random import randint
from dynaconf import Dynaconf
from dynaconf.constants import DEFAULT_SETTINGS_FILES
from forge.service import UnixServer
from .protocol import ServerConnectionType, FromRemotePacketType, ToRemotePacketType, InitiateConnectionStatus

CONFIGURATION = Dynaconf(
    environments=False,
    lowercase_read=False,
    merge_enabled=True,
    default_settings_paths=DEFAULT_SETTINGS_FILES,
)
_LOGGER = logging.getLogger(__name__)


class _RemoteHost:
    class SSHConnection:
        def __init__(self, remote_writer: asyncio.StreamWriter, connection_id: int,
                     reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            self.remote_writer = remote_writer
            self.connection_id: typing.Optional[int] = connection_id
            self.reader = reader
            self.writer = writer
            self._remote_writable = False
            self._incoming_task: typing.Optional[asyncio.Task] = None

        async def open_on_remote(self):
            if self._remote_writable:
                return
            self._remote_writable = True

            self.writer.write(struct.pack('<B', InitiateConnectionStatus.OK.value))
            await self.writer.drain()

        async def data_from_remote(self, data: bytes):
            if not self._remote_writable:
                return
            self.writer.write(data)
            await self.writer.drain()

        def remote_connection_closed(self):
            self.connection_id = None
            if self._incoming_task:
                self._incoming_task.cancel()
                self._incoming_task = None

        async def _dispatch_incoming(self) -> None:
            await self.remote_writer.drain()

            while True:
                data = await self.reader.read(0xFFFF)
                if not data:
                    return
                self.remote_writer.write(struct.pack('<BHH', ToRemotePacketType.DATA, self.connection_id, len(data)))
                self.remote_writer.write(data)
                await self.remote_writer.drain()

        async def run(self):
            self._incoming_task = asyncio.ensure_future(self._dispatch_incoming())
            try:
                await self._incoming_task
            except asyncio.CancelledError:
                pass

        def disconnect(self):
            if self._incoming_task:
                self._incoming_task.cancel()
                self._incoming_task = None

    def __init__(self, host: bytes, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.host = host
        self.reader = reader
        self.writer = writer
        self._incoming_task: typing.Optional[asyncio.Task] = None
        self._ssh_connections: typing.Dict[int, _RemoteHost.SSHConnection] = dict()

    def __repr__(self) -> str:
        return b64encode(self.host).decode('ascii')

    async def _dispatch_incoming(self) -> None:
        while True:
            packet_type = await self.reader.read(1)
            if not packet_type:
                break

            packet_type = FromRemotePacketType(struct.unpack('<B', packet_type)[0])
            if packet_type == FromRemotePacketType.DATA:
                connection_id, data_length = struct.unpack('<HH', await self.reader.readexactly(4))
                packet_data = await self.reader.readexactly(data_length)
                target = self._ssh_connections.get(connection_id)
                if target:
                    await target.data_from_remote(packet_data)
            elif packet_type == FromRemotePacketType.CONNECTION_OPEN:
                connection_id = struct.unpack('<H', await self.reader.readexactly(2))[0]
                target = self._ssh_connections.get(connection_id)
                if target:
                    await target.open_on_remote()
            elif packet_type == FromRemotePacketType.CONNECTION_CLOSED:
                connection_id = struct.unpack('<H', await self.reader.readexactly(2))[0]
                target = self._ssh_connections.pop(connection_id, None)
                if target:
                    target.remote_connection_closed()
            else:
                raise ValueError("Invalid packet type")
        for c in self._ssh_connections.values():
            c.disconnect()
        self._ssh_connections.clear()

    async def run(self):
        self._incoming_task = asyncio.ensure_future(self._dispatch_incoming())
        try:
            await self._incoming_task
        except asyncio.CancelledError:
            pass
        except:
            _LOGGER.debug(f"Error communicating with {self.host}", exc_info=True)

    def disconnect(self):
        if self._incoming_task:
            self._incoming_task.cancel()
            self._incoming_task = None
        for c in self._ssh_connections.values():
            c.disconnect()
        self._ssh_connections.clear()

    async def ssh_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connection_id = randint(0, 0xFFFF)
        increment = 0
        while connection_id in self._ssh_connections:
            increment += 1
            if increment >= 0xFFFF:
                _LOGGER.warning("No available connection identifiers")
                return
            connection_id = (connection_id + 1) % (1 << 16)

        connection = self.SSHConnection(self.writer, connection_id, reader, writer)
        self._ssh_connections[connection_id] = connection
        try:
            self.writer.write(struct.pack('<BH', ToRemotePacketType.SSH_CONNECTION_OPEN, connection_id))
            await connection.run()
        finally:
            if connection.connection_id is not None:
                self._ssh_connections.pop(connection.connection_id, None)
                try:
                    self.writer.write(struct.pack('<BH', ToRemotePacketType.CONNECTION_CLOSE, connection.connection_id))
                    connection.connection_id = None
                    await self.writer.drain()
                except OSError:
                    pass


_active_hosts: typing.Dict[bytes, _RemoteHost] = dict()


class Server(UnixServer):
    DESCRIPTION = "Forge tunnel coordinator server."

    async def connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        _LOGGER.debug("Accepted connection")
        try:
            connection_type = ServerConnectionType(struct.unpack('<B', await reader.readexactly(1))[0])

            if connection_type == ServerConnectionType.INITIATE_CONNECTION:
                target = await reader.readexactly(32)
                host = _active_hosts.get(target)
                if host:
                    _LOGGER.info(f"Initiating connection to {host}")
                    await host.ssh_connection(reader, writer)
                    _LOGGER.info(f"Completed connection to {host}")
                else:
                    host = b64encode(target).decode('ascii')
                    _LOGGER.info(f"No active target found for {host}")
                    writer.write(struct.pack('<B', InitiateConnectionStatus.TARGET_NOT_FOUND.value))
                    await writer.drain()
            elif connection_type == ServerConnectionType.TO_REMOTE:
                host = await reader.readexactly(32)
                prior = _active_hosts.pop(host, None)
                if prior:
                    _LOGGER.info(f"Disconnecting duplicate {prior}")
                    prior.disconnect()

                handler = _RemoteHost(host, reader, writer)
                _LOGGER.info(f"Creating remote target {handler}")
                _active_hosts[host] = handler
                await handler.run()
                _active_hosts.pop(host, None)
                _LOGGER.info(f"Closed remote target {handler}")
            else:
                raise ValueError("Invalid connection type")
        except:
            _LOGGER.debug("Error in connection", exc_info=True)
        finally:
            try:
                writer.close()
            except OSError:
                pass

    @property
    def default_socket(self) -> str:
        return CONFIGURATION.get('TELEMETRY.TUNNEL.SOCKET', '/run/forge-telemetry-tunnel.socket')


def main():
    server = Server()
    server.run()


if __name__ == '__main__':
    main()
