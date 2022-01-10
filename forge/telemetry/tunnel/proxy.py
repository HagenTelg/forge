import typing
import os
import asyncio
import logging
import argparse
import signal
import struct
import sys
from base64 import b64decode, b64encode
from os.path import exists as file_exists
from forge.tasks import background_task
from forge.telemetry import PrivateKey, PublicKey, key_to_bytes
from .protocol import ServerConnectionType, InitiateConnectionStatus


_LOGGER = logging.getLogger(__name__)


async def check_uplink_handshake(reader: asyncio.StreamReader) -> bool:
    try:
        status = await reader.readexactly(1)
    except asyncio.IncompleteReadError:
        print("Connection closed")
        return False
    status = InitiateConnectionStatus(struct.unpack('<B', status)[0])
    if status == InitiateConnectionStatus.OK:
        return True
    elif status == InitiateConnectionStatus.PERMISSION_DENIED:
        print("Permission denied")
        return False
    elif status == InitiateConnectionStatus.TARGET_NOT_FOUND:
        print("Tunnel target not found")
        return False
    else:
        raise ValueError("Invalid handshake status")


async def establish_uplink(parser, args) -> typing.Tuple[typing.Optional[asyncio.StreamReader],
                                                         typing.Optional[asyncio.StreamWriter],
                                                         typing.Optional[asyncio.Task]]:
    if args.url:
        import aiohttp

        if args.key is None:
            parser.error("--key is required with --url")
            return None, None, None

        from starlette.datastructures import URL

        url = URL(url=args.url)
        if url.scheme == 'wss':
            url = url.replace(scheme='https')
        elif url.scheme == 'ws':
            url = url.replace(scheme='http')

        connect_to_key = None
        if args.station is not None:
            if '{station}' in url.path:
                url = url.replace(path=url.path.replace('{station}', args.station))
            elif not url.path.endswith(args.station):
                url = url.replace(path=url.path + '/' + args.station)
        elif args.target is not None:
            connect_to_key = PublicKey.from_public_bytes(b64decode(args.target))
        else:
            parser.error("--station or --connect is required with --url")
            return None, None, None

        key = args.key
        if file_exists(key):
            with open(key, 'rb') as f:
                key = f.read()
            if len(key) == 32:
                key = PrivateKey.from_private_bytes(key)
            else:
                key = PrivateKey.from_private_bytes(b64decode(key.decode('ascii').strip()))
        else:
            key = PrivateKey.from_private_bytes(b64decode(key))

        class WebsocketTransport(asyncio.Transport):
            def __init__(self, websocket: "aiohttp.client.ClientWebSocketResponse", protocol: asyncio.StreamReaderProtocol):
                super().__init__()
                self.websocket = websocket
                self._protocol = protocol
                self._read_task: typing.Optional[asyncio.Task] = None
                self._write_task: typing.Optional[asyncio.Task] = None
                self._write_buffer = bytearray()
                self._closing = False

            def get_protocol(self):
                return self._protocol

            def set_protocol(self, protocol):
                return self._protocol

            async def _write_to_websocket(self, data: bytes):
                await self.websocket.send_bytes(data)
                if len(self._write_buffer):
                    contents = bytes(self._write_buffer)
                    self._write_buffer.clear()
                    self._write_task = background_task(self._write_to_websocket(contents))
                else:
                    self._write_task = None

            def write(self, data: bytes):
                if not self._read_task:
                    raise IOError
                if self._write_task:
                    self._write_buffer += data
                else:
                    self._write_task = background_task(self._write_to_websocket(data))

            async def _drain_helper(self):
                while self._write_task:
                    t = self._write_task
                    await t

            async def _drain_and_close(self):
                await self._drain_helper()
                if self._read_task:
                    try:
                        self._read_task.cancel()
                    except:
                        pass

            def close(self):
                if self._closing:
                    return
                self._closing = True
                background_task(self._drain_and_close())

            write_eof = close

            def is_closing(self):
                return self._closing

            def can_write_eof(self):
                return True

            def abort(self):
                self._closing = True
                if self._read_task:
                    try:
                        self._read_task.cancel()
                    except:
                        pass
                    self._read_task = None

            async def _read_loop(self, ):
                async for msg in self.websocket:
                    if msg.type == aiohttp.WSMsgType.BINARY:
                        self._protocol.data_received(msg.data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        return

            async def run(self):
                self._read_task = asyncio.ensure_future(self._read_loop())
                try:
                    await self._read_task
                except asyncio.CancelledError:
                    pass
                self._protocol.eof_received()

        async def handshake(websocket: "aiohttp.client.ClientWebSocketResponse") -> bool:
            await websocket.send_bytes(key_to_bytes(key.public_key()))
            token = await websocket.receive_bytes()
            signature = key.sign(token)

            if connect_to_key:
                await websocket.send_bytes(signature + key_to_bytes(connect_to_key))
            else:
                await websocket.send_bytes(signature)
            return True

        reader = asyncio.get_event_loop().create_future()
        writer = asyncio.get_event_loop().create_future()

        async def connect():
            _LOGGER.debug(f"Connecting to websocket uplink {url}")
            timeout = aiohttp.ClientTimeout(connect=30, sock_read=60)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.ws_connect(str(url)) as websocket:
                    if not await handshake(websocket):
                        reader.set_result(None)
                        writer.set_result(None)
                        return

                    _LOGGER.debug("Websocket connected")

                    r = asyncio.StreamReader()
                    protocol = asyncio.StreamReaderProtocol(r)
                    transport = WebsocketTransport(websocket, protocol)
                    w = asyncio.StreamWriter(transport, protocol, r, asyncio.get_event_loop())
                    reader.set_result(r)
                    writer.set_result(w)
                    await transport.run()

        handler = asyncio.ensure_future(connect())

        r = await reader
        w = await writer
        if not r or not w:
            return None, None, None
        if not await check_uplink_handshake(r):
            try:
                w.close()
            except OSError:
                pass
            return None, None, None
        return r, w, handler
    elif args.direct:
        from forge.telemetry import CONFIGURATION
        from forge.telemetry.storage import ControlInterface

        hub_socket = args.hub_socket
        if hub_socket is None:
            hub_socket = CONFIGURATION.get('TELEMETRY.TUNNEL.SOCKET', '/run/forge-telemetry-tunnel.socket')

        if args.station is not None:
            database_uri = args.database_uri
            if database_uri is None:
                database_uri = CONFIGURATION.TELEMETRY.DATABASE

            connect_to_key = await ControlInterface(database_uri).tunnel_station_to_public_key(args.station)
            if connect_to_key is None:
                parser.error(f"Station {args.station} not found in telemetry")

            _LOGGER.debug(f"Resolved {args.station} to {b64encode(key_to_bytes(connect_to_key))}")
        elif args.target is not None:
            connect_to_key = PublicKey.from_public_bytes(b64decode(args.target))
        else:
            parser.error("--station or --connect is required with --direct")
            return None, None, None

        _LOGGER.debug(f"Establishing direct connection to {b64encode(key_to_bytes(connect_to_key))}")

        reader, writer = await asyncio.open_unix_connection(hub_socket)
        writer.write(struct.pack('<B', ServerConnectionType.INITIATE_CONNECTION.value))
        writer.write(key_to_bytes(connect_to_key))
        await writer.drain()

        _LOGGER.debug("Direct connection open")

        if not await check_uplink_handshake(reader):
            try:
                writer.close()
            except OSError:
                pass
            return None, None, None
        return reader, writer, None
    else:
        parser.error("--url or --direct must be specified")
        return None, None, None


async def establish_downlink(parser, args) -> typing.Tuple[typing.Optional[asyncio.StreamReader],
                                                           typing.Optional[asyncio.StreamWriter],
                                                           typing.Optional[asyncio.Task]]:
    if args.proxy:
        loop = asyncio.get_event_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)
        w_transport, w_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, sys.stdout)
        writer = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)
        return reader, writer, None
    elif args.launch:
        process = await asyncio.create_subprocess_shell(args.launch,
                                                        stdin=asyncio.subprocess.PIPE,
                                                        stdout=asyncio.subprocess.PIPE)
        if not process:
            return None, None, None
        task = asyncio.ensure_future(process.wait())
        return process.stdout, process.stdin, task
    elif args.listen is not None:
        reader = asyncio.get_event_loop().create_future()
        writer = asyncio.get_event_loop().create_future()

        async def connection(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
            reader.set_result(r)
            writer.set_result(w)

        server = await asyncio.start_server(connection, port=args.listen, host=args.listen_host)
        r = await reader
        w = await writer
        server.close()
        return r, w, None
    else:
        parser.error("--proxy, --listen, or --launch must be specified")
        return None, None, None


def forward_stream(source: asyncio.StreamReader, destination: asyncio.StreamWriter) -> asyncio.Task:
    async def _task():
        while True:
            data = await source.read(4096)
            if not data:
                return
            destination.write(data)
            await destination.drain()

    return asyncio.ensure_future(_task())


async def run(parser, args):
    uplink_read, uplink_write, uplink_task = await establish_uplink(parser, args)
    if not uplink_read or not uplink_write:
        return
    _LOGGER.debug("Uplink ready")
    try:
        downlink_read, downlink_write, downlink_task = await establish_downlink(parser, args)
        if not downlink_read or not downlink_write:
            uplink_write.close()
            return
        _LOGGER.debug("Downlink ready")

        try:
            _LOGGER.debug("Starting stream forwarding")
            _, pending = await asyncio.wait([
                forward_stream(uplink_read, downlink_write),
                forward_stream(downlink_read, uplink_write)
            ], return_when=asyncio.FIRST_COMPLETED)
            _LOGGER.debug("Stream forwarding completed")
            for t in pending:
                try:
                    t.cancel()
                    await t
                except:
                    pass
        finally:
            try:
                downlink_write.close()
            except OSError:
                pass

            if downlink_task:
                try:
                    await downlink_task
                except:
                    pass
    finally:
        try:
            uplink_write.close()
        except OSError:
            pass
        
        if uplink_task:
            try:
                await uplink_task                
            except:
                pass        

def main():
    parser = argparse.ArgumentParser(description="Tunnel proxy interface.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--key',
                        dest='key',
                        help="system key file")
    parser.add_argument('--station',
                        dest='station',
                        help="system key file")
    parser.add_argument('--connect',
                        dest='target',
                        help="target key identifier")
    parser.add_argument('--listen-host',
                        dest='listen_host',
                        help="TCP host to listen on",
                        default='localhost')
    parser.add_argument('--database',
                        dest='database_uri',
                        help="backend database URI")
    parser.add_argument('--hub-socket',
                        dest='hub_socket',
                        help="direct connection hub server socket")

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--listen',
                       dest='listen', type=int,
                       help="TCP port to listen on")
    group.add_argument('--proxy', action='store_true',
                       dest='proxy',
                       help="proxy stdio to the remote target")
    group.add_argument('--launch',
                       dest='launch',
                       help="command to launch with stdio proxied")

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--direct', action='store_true',
                       dest='direct',
                       help="directly connect to the hub server")
    group.add_argument('--url',
                       help="telemetry server websocket URL")

    args = parser.parse_args()
    if args.debug:
        root_logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)-40s %(message)s')
        handler.setFormatter(formatter)
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, loop.stop)
    loop.add_signal_handler(signal.SIGTERM, loop.stop)
    loop.run_until_complete(run(parser, args))
    loop.close()


if __name__ == '__main__':
    main()