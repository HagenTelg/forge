import typing
import asyncio
import logging
import argparse
import signal
import aiohttp
import time
import random
from base64 import b64decode
from json import loads as from_json, dumps as to_json
from os.path import exists as file_exists
from dynaconf import Dynaconf
from dynaconf.constants import DEFAULT_SETTINGS_FILES
from starlette.datastructures import URL
from forge.tasks import background_task
from forge.authsocket import WebsocketJSON as AuthSocket, PrivateKey
from forge.telemetry.assemble import complete as assemble_complete_telemetry
from forge.telemetry.assemble.station import get_station

CONFIGURATION = Dynaconf(
    environments=False,
    lowercase_read=False,
    merge_enabled=True,
    default_settings_paths=DEFAULT_SETTINGS_FILES,
)
_LOGGER = logging.getLogger(__name__)


_last_full_telemetry: typing.Optional[float] = None


class UplinkConnection:
    def __init__(self, key: PrivateKey, url: URL, args: argparse.Namespace):
        self.key = key
        self.url = url
        self.args = args
        self.websocket: "aiohttp.client.ClientWebSocketResponse" = None
        self._tasks: typing.List[asyncio.Task] = list()

    def _handle_server_time(self, data: typing.Dict[str, typing.Any]) -> None:
        if self.args.time_output is not None:
            try:
                server_time = int(data.get('server_time'))
                local_time = round(time.time())

                _LOGGER.debug(
                    f"Writing time file {self.args.time_output} with local time {local_time} and server time {server_time}")
                try:
                    with open(self.args.time_output, 'w') as f:
                        f.write(to_json({
                            'server_time': server_time,
                            'local_time': local_time,
                        }))
                except OSError:
                    _LOGGER.warning(f"Error writing time file {self.args.time_output}", exc_info=True)
            except (ValueError, TypeError):
                pass

    async def _poll_server_time(self):
        while True:
            await self.websocket.send_json({
                'request': 'get_time',
            })
            if self.args.level > 1:
                await asyncio.sleep(random.uniform(60, 120))
            else:
                await asyncio.sleep(random.uniform(600, 1200))

    async def _send_complete_telemetry(self):
        global _last_full_telemetry

        if _last_full_telemetry is None:
            await self.websocket.send_json({
                'request': 'update',
                'telemetry': await assemble_complete_telemetry(),
            })
            _last_full_telemetry = time.monotonic()
        else:
            time_since_send = time.monotonic() - _last_full_telemetry
            if time_since_send > 3600:
                await self.websocket.send_json({
                    'request': 'update',
                    'telemetry': await assemble_complete_telemetry(),
                })
                _last_full_telemetry = time.monotonic()

        while True:
            if self.args.level > 1:
                await asyncio.sleep(random.uniform(3600, 7200))
            else:
                target_time = int(_last_full_telemetry / 86400) * 86400 + 86400
                delay = target_time - time.monotonic()
                delay += random.uniform(0, 7200)
                if delay > 0:
                    await asyncio.sleep(delay)
            await self.websocket.send_json({
                'request': 'update',
                'telemetry': await assemble_complete_telemetry(),
            })
            _last_full_telemetry = time.monotonic()

    async def _send_basic_telemetry(self):
        from forge.telemetry.assemble.memory import add_memory_utilization
        from forge.telemetry.assemble.disk import add_disk_rate
        from forge.telemetry.assemble.cpu import add_cpu_utilization
        from forge.telemetry.assemble.services import add_failed_services

        await asyncio.sleep(random.uniform(1, 600))
        while True:
            telemetry = {}
            await add_memory_utilization(telemetry)
            await add_disk_rate(telemetry)
            await add_cpu_utilization(telemetry)
            await add_failed_services(telemetry)
            await self.websocket.send_json({
                'request': 'partial',
                'telemetry': telemetry,
            })
            await asyncio.sleep(random.uniform(300, 900))

    async def _send_verbose_telemetry(self):
        from forge.telemetry.assemble.time import add_time
        from forge.telemetry.assemble.network import add_network_rate
        from forge.telemetry.assemble.memory import add_memory_utilization
        from forge.telemetry.assemble.disk import add_disk_rate, add_disk_space
        from forge.telemetry.assemble.cpu import add_cpu_utilization
        from forge.telemetry.assemble.services import add_failed_services

        await asyncio.sleep(random.uniform(1, 60))
        while True:
            telemetry = {}
            await add_time(telemetry)
            await add_network_rate(telemetry)
            await add_memory_utilization(telemetry)
            await add_disk_rate(telemetry)
            await add_disk_space(telemetry)
            await add_cpu_utilization(telemetry)
            await add_failed_services(telemetry)
            await self.websocket.send_json({
                'request': 'partial',
                'telemetry': telemetry,
            })
            await asyncio.sleep(60)

    async def _stream_log(self, name: str, task: asyncio.Task, source: typing.AsyncGenerator):
        self._tasks.append(task)

        async def _stream():
            _queued: typing.List[typing.Dict[str, typing.Any]] = list()
            _next_send = time.monotonic()
            _send_task: typing.Optional[asyncio.Task] = None

            def _schedule_send():
                nonlocal _send_task
                if _send_task is not None:
                    return
                if len(_queued) == 0:
                    return
                delay = _next_send - time.monotonic()
                if delay <= 0.0:
                    delay = 0.1
                _send_task = asyncio.ensure_future(_empty_queue(delay))

            async def _empty_queue(delay: float):
                nonlocal _send_task

                await asyncio.sleep(delay)

                if not self.websocket:
                    _send_task = None
                    return

                to_send = list(_queued)
                _queued.clear()
                _next_send = time.monotonic() + 1.0

                await self.websocket.send_json({
                    'request': 'log',
                    'log': name,
                    'events': to_send,
                })
                _send_task = None
                _schedule_send()

            async for event in source:
                _queued.append(event)
                del _queued[:-10]
                _schedule_send()

        self._tasks.append(asyncio.ensure_future(_stream()))

    async def _stream_kernel_log(self):
        from forge.telemetry.assemble.logs import stream_kernel_log

        task, source = await stream_kernel_log()
        await self._stream_log('kernel', task, source)

    async def _stream_acquisition_log(self):
        from forge.telemetry.assemble.logs import stream_kernel_log

        task, source = await stream_kernel_log()
        await self._stream_log('acquisition', task, source)

    async def _dispatch_incoming(self, data: typing.Dict[str, typing.Any]) -> None:
        if data.get('response') == 'server_time':
            self._handle_server_time(data)

    async def run(self):
        try:
            timeout = aiohttp.ClientTimeout(connect=30, sock_read=60)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.ws_connect(str(self.url)) as websocket:
                    self.websocket = websocket
                    await AuthSocket.client_handshake(self.websocket, self.key, extra_data={
                        'station': await get_station(),
                    })
                    _LOGGER.info(f"Telemetry uplink connected to {self.url}")

                    self._tasks.append(asyncio.ensure_future(self._poll_server_time()))
                    self._tasks.append(asyncio.ensure_future(self._send_complete_telemetry()))
                    if self.args.level > 1:
                        self._tasks.append(asyncio.ensure_future(self._send_verbose_telemetry()))
                        await self._stream_kernel_log()
                        await self._stream_acquisition_log()
                    elif self.args.level > 0:
                        self._tasks.append(asyncio.ensure_future(self._send_basic_telemetry()))

                    async for msg in self.websocket:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._dispatch_incoming(from_json(msg.data))
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            await self._dispatch_incoming(from_json(msg.data.decode('utf-8')))
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            return
        finally:
            self.websocket = None

    async def disconnect(self) -> None:
        for t in list(self._tasks):
            try:
                t.cancel()
                await t
            except:
                pass
        self.websocket = None


async def run(key: PrivateKey, url: URL, args: argparse.Namespace):
    while True:
        uplink = UplinkConnection(key, url, args)
        try:
            try:
                await uplink.run()
            except:
                _LOGGER.info(f"Connection to {url} terminated", exc_info=True)
        finally:
            try:
                await uplink.disconnect()
            except:
                _LOGGER.debug(f"Uplink disconnection error", exc_info=True)
        await asyncio.sleep(60)


def main():
    parser = argparse.ArgumentParser(description="Telemetry websocket remote uplink.")

    default_url = CONFIGURATION.get('TELEMETRY.URL')
    if default_url:
        if isinstance(default_url, str):
            default_url = [default_url]
        elif not isinstance(default_url, list):
            default_url = None
        if default_url:
            for url in default_url:
                url = URL(url=url)

                if url.scheme == 'ws' or url.scheme == 'wss':
                    pass
                elif url.scheme == 'http' or url.scheme == 'https':
                    # Path will be something like: '/update', and we need '/socket/update'
                    url = url.replace(path=url.path[:-6] + 'socket/update')
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
    parser.add_argument('--key',
                        dest='key',
                        help="system key file")

    parser.add_argument('--time-output',
                        dest='time_output',
                        help="output time file",
                        default=CONFIGURATION.get('TELEMETRY.TIME_OUTPUT'))
    parser.add_argument('--level',
                        dest='level', type=int,
                        help="telemetry level",
                        default=int(CONFIGURATION.get('TELEMETRY.LEVEL', 1)))

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
    _LOGGER.info(f"Telemetry uplink to {url} starting")
    if url.scheme == 'wss':
        url = url.replace(scheme='https')
    elif url.scheme == 'ws':
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

    background_task(run(key, url, args))
    loop.add_signal_handler(signal.SIGINT, loop.stop)
    loop.add_signal_handler(signal.SIGTERM, loop.stop)
    loop.run_forever()


if __name__ == '__main__':
    main()
