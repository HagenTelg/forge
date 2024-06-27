import time
import typing
import asyncio
import logging
import argparse
import signal
import aiohttp
import struct
import os
from base64 import b64decode
from os.path import exists as file_exists
from dynaconf import Dynaconf
from dynaconf.constants import DEFAULT_SETTINGS_FILES
from starlette.datastructures import URL
from forge.tasks import background_task, wait_cancelable
from forge.authsocket import WebsocketBinary as AuthSocket, PrivateKey
from forge.cpd3.variant import deserialize as deserialize_variant, serialize as serialize_variant
from forge.cpd3.variant import Metadata, MetadataChildren, MetadataString, MetadataInteger, MetadataBytes, MetadataBoolean
from forge.cpd3.acquisition.client import Client as BaseCPD3Acquisition, RealtimeName
from forge.cpd3.acquisition.incoming.protocol import PROTOCOL_VERSION, PacketToAcquisition, PacketFromAcquisition, DataBlockType


CONFIGURATION = Dynaconf(
    environments=False,
    lowercase_read=False,
    merge_enabled=True,
    default_settings_paths=DEFAULT_SETTINGS_FILES,
)
_LOGGER = logging.getLogger(__name__)


class CPD3Acquisition(BaseCPD3Acquisition):
    CONNECTION_TIMEOUT = 11.0

    class _RatelimitSend:
        def __init__(self, delay=0.0, rate=1.0):
            self._sender: typing.Optional[typing.Callable[[], typing.Awaitable]] = None
            self._send_task: typing.Optional[asyncio.Task] = None
            self._next_send: float = time.monotonic() + delay
            self._rate = rate
            self._lock = asyncio.Lock()
            self._canceled = False

        async def _do_send(self):
            async with self._lock:
                if self._canceled:
                    return
                if not self._sender:
                    return

                now = time.monotonic()
                delay = self._next_send - now
                self._next_send = now + self._rate

            if delay > 0.0:
                await asyncio.sleep(delay)

            async with self._lock:
                if self._canceled:
                    return

                sender = self._sender
                self._sender = None
                self._send_task = None

            if not sender:
                return
            await sender()

        async def update(self, sender: typing.Callable[[], typing.Awaitable]) -> None:
            async with self._lock:
                if self._canceled:
                    return

                self._sender = sender
                if self._send_task:
                    return

                self._send_task = background_task(self._do_send())

        async def cancel(self):
            async with self._lock:
                self._canceled = True
                to_cancel = self._send_task
                self._send_task = None

            if to_cancel:
                try:
                    to_cancel.cancel()
                except:
                    pass
                try:
                    await to_cancel
                except:
                    pass

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, uplink: "UplinkConnection"):
        super().__init__(reader, writer)
        self.uplink = uplink
        self._ready_waiter: typing.Optional[asyncio.Future] = asyncio.get_event_loop().create_future()
        self._uplink_neutral = asyncio.Lock()

        self._event_ratelimit_count: int = 0
        self._event_ratelimit_reset: typing.Optional[float] = None

        self._autoprobe_state_ratelimit = self._RatelimitSend()
        self._interface_information_ratelimit: typing.Dict[str, "CPD3Acquisition._RatelimitSend"] = dict()
        self._interface_state_ratelimit: typing.Dict[str, "CPD3Acquisition._RatelimitSend"] = dict()

    async def connection_ready(self) -> None:
        if not self._ready_waiter:
            return
        self._ready_waiter.set_result(None)

    async def wait_for_connected(self) -> None:
        if not self._ready_waiter:
            return
        await self._ready_waiter
        self._ready_waiter = None

    async def disconnect(self) -> None:
        await self._autoprobe_state_ratelimit.cancel()
        for c in self._interface_information_ratelimit.values():
            await c.cancel()
        for c in self._interface_state_ratelimit.values():
            await c.cancel()

    def _apply_event_ratelimit(self) -> bool:
        now = time.monotonic()
        if self._event_ratelimit_reset is None or self._event_ratelimit_reset < now:
            self._event_ratelimit_reset = now + 1
            self._event_ratelimit_count = 0
            return True
        self._event_ratelimit_count += 1
        return self._event_ratelimit_count < 20

    async def incoming_event(self, event: typing.Any) -> None:
        # Background this, since we don't want to block the socket handling if we're in the middle of a data upload
        async def send():
            async with self.uplink.uplink_neutral:
                if not self.uplink.websocket:
                    return
                if not self._apply_event_ratelimit():
                    return
                await self.uplink.websocket.send_bytes(struct.pack('<B', PacketFromAcquisition.EVENT.value) +
                                                       serialize_variant(event))
        background_task(send())

    async def incoming_autoprobe_state(self, state: typing.Any) -> None:
        async def send():
            async with self.uplink.uplink_neutral:
                if not self.uplink.websocket:
                    return
                await self.uplink.websocket.send_bytes(struct.pack('<B', PacketFromAcquisition.AUTOPROBE_STATE.value) +
                                                       serialize_variant(state))

        await self._autoprobe_state_ratelimit.update(send)

    async def _send_string_and_data(self, packet_type: PacketFromAcquisition, s: str, data: typing.Any) -> None:
        async with self.uplink.uplink_neutral:
            if not self.uplink.websocket:
                return
            raw = s.encode('utf-8')
            await self.uplink.websocket.send_bytes(struct.pack('<BH', packet_type.value, len(raw)) +
                                                   raw + serialize_variant(data))

    async def incoming_interface_information(self, interface: str, info: typing.Any) -> None:
        async def send():
            await self._send_string_and_data(PacketFromAcquisition.INTERFACE_INFORMATION, interface, info)

        ratelimit = self._interface_information_ratelimit.get(interface)
        if not ratelimit:
            ratelimit = self._RatelimitSend()
            self._interface_information_ratelimit[interface] = ratelimit

        await ratelimit.update(send)

    async def incoming_interface_state(self, interface: str, state: typing.Any) -> None:
        async def send():
            await self._send_string_and_data(PacketFromAcquisition.INTERFACE_STATE, interface, state)

        ratelimit = self._interface_state_ratelimit.get(interface)
        if not ratelimit:
            ratelimit = self._RatelimitSend()
            self._interface_state_ratelimit[interface] = ratelimit

        await ratelimit.update(send)

    async def incoming_value(self, name: RealtimeName, value: typing.Any) -> None:
        await self.uplink.realtime_value(name, value)


class UplinkConnection:
    def __init__(self, key: PrivateKey, url: URL, args: argparse.Namespace):
        self.key = key
        self.url = url
        self.args = args
        self.include_instantaneous = args.include_instantaneous
        self.websocket: "aiohttp.client.ClientWebSocketResponse" = None
        self.acquisition: CPD3Acquisition = None

        self.uplink_neutral = asyncio.Lock()
        self._pending_values: typing.Dict[RealtimeName, typing.Any] = dict()
        self._value_flush_task: typing.Optional[asyncio.Task] = None
        self._realtime_name_index: typing.Dict[RealtimeName, int] = dict()
        self._realtime_index_name: typing.List[RealtimeName] = list()
        self._realtime_index_replace = 0

        self._unsmoothed_names: typing.Set[RealtimeName] = set()
        self._persistent_names: typing.Set[RealtimeName] = set()

    async def _websocket_packet(self, data: bytes) -> None:
        packet_type = PacketToAcquisition(data[0])
        if packet_type == PacketToAcquisition.MESSAGE_LOG:
            if self.acquisition:
                await self.acquisition.message_log(deserialize_variant(data[1:]))
        elif packet_type == PacketToAcquisition.COMMAND:
            if self.acquisition:
                data = bytearray(data[1:])
                target_length = (struct.unpack('<H', data[:2]))[0]
                del data[:2]
                target = data[:target_length].decode('utf-8')
                del data[:target_length]
                command = deserialize_variant(data)
                await self.acquisition.command(command=command, target=target)
        elif packet_type == PacketToAcquisition.BYPASS_FLAG_SET:
            if self.acquisition:
                await self.acquisition.bypass(data[1:].decode('utf-8'))
        elif packet_type == PacketToAcquisition.BYPASS_FLAG_CLEAR:
            if self.acquisition:
                await self.acquisition.unbypass(data[1:].decode('utf-8'))
        elif packet_type == PacketToAcquisition.BYPASS_FLAGS_CLEAR_ALL:
            if self.acquisition:
                await self.acquisition.unbypass_override()
        elif packet_type == PacketToAcquisition.SYSTEM_FLAG_SET:
            if self.acquisition:
                await self.acquisition.flag(data[1:].decode('utf-8'))
        elif packet_type == PacketToAcquisition.SYSTEM_FLAG_CLEAR:
            if self.acquisition:
                await self.acquisition.unflag(data[1:].decode('utf-8'))
        elif packet_type == PacketToAcquisition.SYSTEM_FLAGS_CLEAR_ALL:
            if self.acquisition:
                await self.acquisition.unflag_override()
        elif packet_type == PacketToAcquisition.SYSTEM_FLUSH:
            if self.acquisition:
                await self.acquisition.system_flush((struct.unpack('<d', data))[0])
        elif packet_type == PacketToAcquisition.RESTART_ACQUISITION_SYSTEM:
            if self.acquisition:
                await self.acquisition.restart_acquisition_system()
        else:
            raise ValueError(f"Invalid packet type {packet_type}")

    async def _read_websocket(self) -> None:
        async for msg in self.websocket:
            if msg.type == aiohttp.WSMsgType.BINARY:
                await self._websocket_packet(msg.data)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                return

    def _allocate_realtime_name(self, name: RealtimeName) -> typing.Tuple[int, typing.Optional[RealtimeName]]:
        index = len(self._realtime_index_name)
        if index <= 0xFFFF:
            self._realtime_index_name.append(name)
            self._realtime_name_index[name] = index
            return index, None

        index = self._realtime_index_replace
        self._realtime_index_replace = (self._realtime_index_replace + 1) & 0xFFFF
        replaced = self._realtime_index_name[index]
        del self._realtime_name_index[replaced]

        self._realtime_index_name[index] = name
        self._realtime_name_index[name] = index
        return index, replaced

    async def _flush_values(self) -> None:
        await asyncio.sleep(1.0)

        values_to_flush = list(self._pending_values.items())
        self._pending_values.clear()
        self._value_flush_task = None

        queued_names = bytearray()
        queued_floats = bytearray()
        queued_arrays_of_floats = bytearray()
        queued_variants = bytearray()

        async def flush_queued():
            nonlocal queued_names
            nonlocal queued_floats
            nonlocal queued_arrays_of_floats
            nonlocal queued_variants

            if len(queued_names) > 0:
                packet = bytes()
                packet += struct.pack('<B', PacketFromAcquisition.DEFINE_NAMES.value)
                packet += queued_names
                queued_names.clear()
                if not self.websocket:
                    return
                await self.websocket.send_bytes(packet)

            if len(queued_floats) == 0 and len(queued_arrays_of_floats) == 0 and len(queued_variants) == 0:
                return

            packet = bytes()
            packet += struct.pack('<B', PacketFromAcquisition.DATA_BLOCK_BEGIN.value)

            if len(queued_floats) > 0:
                packet += struct.pack('<B', DataBlockType.FLOATS.value)
                packet += queued_floats
                queued_floats.clear()
                if not self.websocket:
                    return
                await self.websocket.send_bytes(packet)
                packet = bytes()

            if len(queued_arrays_of_floats) > 0:
                packet += struct.pack('<B', DataBlockType.ARRAYS_OF_FLOATS.value)
                packet += queued_arrays_of_floats
                queued_arrays_of_floats.clear()
                if not self.websocket:
                    return
                await self.websocket.send_bytes(packet)
                packet = bytes()

            if len(queued_variants) > 0:
                packet += struct.pack('<B', DataBlockType.VARIANT.value)
                packet += queued_variants
                queued_variants.clear()
                if not self.websocket:
                    return
                await self.websocket.send_bytes(packet)
                packet = bytes()

            packet += struct.pack('<B', DataBlockType.FINAL.value)
            if not self.websocket:
                return
            await self.websocket.send_bytes(packet)

        def enqueue_value(name_index: int, value: typing.Any):
            nonlocal queued_floats
            nonlocal queued_arrays_of_floats
            nonlocal queued_variants

            if isinstance(value, float):
                try:
                    queued_floats += struct.pack('<Hf', name_index, value)
                    return
                except OverflowError:
                    pass

            if isinstance(value, list):
                contents = bytearray()
                all_ok = True
                for f in value:
                    if not isinstance(f, float):
                        all_ok = False
                        break
                    try:
                        contents += struct.pack('<f', f)
                    except OverflowError:
                        all_ok = False
                        break
                if all_ok:
                    queued_arrays_of_floats += struct.pack('<H', name_index)
                    if len(value) < 0xFF:
                        queued_arrays_of_floats += struct.pack('<B', len(value))
                    else:
                        queued_arrays_of_floats += struct.pack('<BI', 0xFF, len(value))
                    queued_arrays_of_floats += contents
                    return

            queued_variants += struct.pack('<H', name_index)
            queued_variants += serialize_variant(value)

        def queue_full() -> bool:
            if len(queued_names) > 4096:
                return True
            if len(queued_floats) > 4096:
                return True
            if len(queued_arrays_of_floats) > 4096:
                return True
            if len(queued_variants) > 4096:
                return True
            return False

        async with self.uplink_neutral:
            used_names: typing.Set[RealtimeName] = set()
            for name, value in values_to_flush:
                index = self._realtime_name_index.get(name)
                if index is None:
                    index, replaced = self._allocate_realtime_name(name)
                    if replaced is not None and replaced in used_names:
                        await flush_queued()
                        used_names.clear()
                    queued_names += name.serialize()

                enqueue_value(index, value)
                used_names.add(name)
                if queue_full():
                    await flush_queued()
                    used_names.clear()
            await flush_queued()

    def _metadata_disables_smoothing(self, value: typing.Any) -> bool:
        if isinstance(value, MetadataString):
            return True
        if isinstance(value, MetadataInteger):
            return True
        if isinstance(value, MetadataBytes):
            return True
        if isinstance(value, MetadataBoolean):
            return True

        if isinstance(value, Metadata):
            smoothing_info = value.get('Smoothing')
            if smoothing_info:
                mode = smoothing_info.get('Mode', '').lower()
                if mode == 'bypass' or mode == 'none':
                    return True

        if isinstance(value, MetadataChildren):
            return self._metadata_disables_smoothing(value.get('Children'))

        return False

    def _metadata_enables_persistence(self, value: typing.Any) -> bool:
        if isinstance(value, Metadata):
            realtime_info = value.get('Realtime')
            if realtime_info:
                return bool(realtime_info.get('Persistent'))

        if isinstance(value, MetadataChildren):
            return self._metadata_enables_persistence(value.get('Children'))

        return False

    def _inspect_value(self, name: RealtimeName, value: typing.Any) -> None:
        if name.archive == 'raw_meta' or name.archive == 'rt_instant_meta':
            base_name = RealtimeName(name.station, name.archive[:-5], name.variable, name.flavors)

            if self._metadata_disables_smoothing(value):
                self._unsmoothed_names.add(base_name)
            else:
                self._unsmoothed_names.discard(base_name)

            if self._metadata_enables_persistence(value):
                self._persistent_names.add(base_name)
            else:
                self._persistent_names.discard(base_name)

    def _instantaneous_as_raw(self, name: RealtimeName) -> bool:
        # Persistent values do not update in raw averages immediately
        if name in self._persistent_names:
            return True
        return False

    def _send_value_to_uplink(self, name: RealtimeName) -> bool:
        # Metadata explicitly excluded
        if name.archive == 'raw':
            return True
        if not self.include_instantaneous:
            return False
        if name.archive == 'rt_instant':
            return True
        return False

    def _defer_value_flush(self,  name: RealtimeName) -> bool:
        # if name.variable.startswith('ZSTATE_'):
        #     return False
        # Unsmoothed values get sent, but do not trigger the flush
        if name in self._unsmoothed_names:
            return True
        return False

    async def _dispatch_value(self, name: RealtimeName, value: typing.Any) -> None:
        if not self._send_value_to_uplink(name):
            return
        if not self.websocket:
            return

        self._pending_values[name] = value

        if self._defer_value_flush(name):
            return
        if self._value_flush_task:
            return
        self._value_flush_task = asyncio.get_event_loop().create_task(self._flush_values())

    async def realtime_value(self, name: RealtimeName, value: typing.Any) -> None:
        self._inspect_value(name, value)

        if name.archive == 'rt_instant':
            if self._instantaneous_as_raw(name):
                await self._dispatch_value(RealtimeName(name.station, 'raw', name.variable, name.flavors), value)
        elif name.archive == 'raw':
            if self._instantaneous_as_raw(name):
                # Received from rt_instant already
                return

        await self._dispatch_value(name, value)

    async def run(self):
        acquisition_task: typing.Optional[asyncio.Task] = None
        acquisition_connection: typing.Optional[asyncio.StreamWriter] = None
        websocket_task: typing.Optional[asyncio.Task] = None
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

                    cpd3_socket = self.args.cpd3_socket
                    if not cpd3_socket.startswith('/'):
                        parts = cpd3_socket.split()
                        host = parts[0]
                        if not host:
                            host = 'localhost'
                        if len(parts) > 1:
                            port = int(parts[1])
                        else:
                            port = 14234
                        _LOGGER.debug(f"Connecting to remote CPD3 socket {host} on {port}")
                        try:
                            reader, writer = await asyncio.open_connection(host=host, port=port)
                        except ConnectionError:
                            _LOGGER.warning(f"Error connecting acquisition socket {cpd3_socket}", exc_info=True)
                            return
                    else:
                        _LOGGER.debug(f"Connecting to local CPD3 socket {cpd3_socket}")
                        try:
                            reader, writer = await asyncio.open_unix_connection(cpd3_socket)
                        except ConnectionError:
                            _LOGGER.warning(f"Error connecting acquisition socket {cpd3_socket}", exc_info=True)
                            return
                    acquisition_connection = writer
                    _LOGGER.debug(f"Acquisition socket connected to {cpd3_socket}")

                    self.acquisition = CPD3Acquisition(reader, writer, self)
                    acquisition_task = asyncio.ensure_future(self.acquisition.run())
                    await wait_cancelable(self.acquisition.wait_for_connected(), timeout=10.0)
                    _LOGGER.info(f"Uplink connected from {cpd3_socket} to {self.url}")

                    websocket_task = asyncio.ensure_future(self._read_websocket())

                    await asyncio.wait([acquisition_task, websocket_task], return_when=asyncio.FIRST_COMPLETED)
        finally:
            self.websocket = None

            if websocket_task:
                try:
                    websocket_task.cancel()
                except:
                    pass
                try:
                    await websocket_task
                except:
                    pass

            flush_task = self._value_flush_task
            self._value_flush_task = None
            if flush_task:
                try:
                    flush_task.cancel()
                except:
                    pass

            if acquisition_task:
                try:
                    acquisition_task.cancel()
                except:
                    pass
                try:
                    await acquisition_task
                except:
                    pass
            if self.acquisition:
                await self.acquisition.disconnect()
            self.acquisition = None
            if acquisition_connection:
                try:
                    acquisition_connection.close()
                except:
                    pass


async def run(key: PrivateKey, url: URL, args: argparse.Namespace):
    while True:
        uplink = UplinkConnection(key, url, args)
        try:
            await uplink.run()
        except:
            _LOGGER.info(f"Connection to {url} terminated", exc_info=True)
        await asyncio.sleep(60)


def main():
    parser = argparse.ArgumentParser(description="CPD3 acquisition uplink.")

    default_station = os.environ.get('CPD3STATION', 'nil').lower()
    default_url = CONFIGURATION.get('CPD3.ACQUISITION.UPLINK_URL')
    if default_url:
        url = URL(url=default_url)
        if '{station}' in url.path:
            url = url.replace(path=url.path.replace('{station}', default_station))
        elif default_station and not url.path.endswith(f'/{default_station}'):
            url = url.replace(path=f'{url.path}/{default_station}')
        default_url = str(url)

    parser.add_argument('url',
                        help="CPD3 acquisition uplink websocket URL",
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
    parser.set_defaults(include_instantaneous=bool(CONFIGURATION.get('CPD3.ACQUISITION.INCLUDE_INSTANTANEOUS', False)))

    parser.add_argument('--cpd3-socket',
                        dest='cpd3_socket', type=str,
                        default=CONFIGURATION.get('CPD3.ACQUISITION.NATIVE_SOCKET', '/tmp/CPD3Acquisition'),
                        help="acquisition system native socket")

    args = parser.parse_args()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

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

    if args.systemd:
        import systemd.daemon

        async def heartbeat():
            systemd.daemon.notify("READY=1")
            while True:
                await asyncio.sleep(10)
                systemd.daemon.notify("WATCHDOG=1")

        background_task(heartbeat())

    background_task(run(key, url, args))
    loop.add_signal_handler(signal.SIGINT, loop.stop)
    loop.add_signal_handler(signal.SIGTERM, loop.stop)
    loop.run_forever()


if __name__ == '__main__':
    main()
