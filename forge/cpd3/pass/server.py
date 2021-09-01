import typing
import os
import asyncio
import logging
import argparse
import struct
import signal
from math import isfinite
from dynaconf import Dynaconf
from dynaconf.constants import DEFAULT_SETTINGS_FILES

CONFIGURATION = Dynaconf(
    environments=False,
    lowercase_read=False,
    merge_enabled=True,
    default_settings_paths=DEFAULT_SETTINGS_FILES,
)
_LOGGER = logging.getLogger(__name__)


_interface = CONFIGURATION.get('CPD3.INTERFACE', 'cpd3_forge_interface')


class _PassOperation:
    def __init__(self, start_epoch: int, end_epoch: int, station: str, profile: str, comment: str):
        self.start_epoch = start_epoch
        self.end_epoch = end_epoch
        self.station = station
        self.profile = profile
        self.comment = comment
        self._done = asyncio.Event()

    async def update(self) -> None:
        _LOGGER.info(f"Starting passed update for {self.station} {self.profile}")
        process = await asyncio.create_subprocess_exec(_interface, "pass_update",
                                                       self.station, self.profile,
                                                       stdout=asyncio.subprocess.DEVNULL,
                                                       stdin=asyncio.subprocess.DEVNULL)
        await process.wait()
        self._done.set()
        if process.returncode != 0:
            _LOGGER.warning(f"Error updating passed data for {self.station} {self.profile}, return code {process.returncode}")
            return
        _LOGGER.info(f"Passed update completed for {self.station} {self.profile}")

    async def apply(self) -> None:
        process = await asyncio.create_subprocess_exec(_interface, "pass_data",
                                                       str(self.start_epoch), str(self.end_epoch),
                                                       self.station, self.profile,
                                                       stdout=asyncio.subprocess.DEVNULL,
                                                       stdin=asyncio.subprocess.PIPE)
        comment = self.comment.encode('utf-8')
        try:
            process.stdin.write(struct.pack('<I', len(comment)))
            process.stdin.write(comment)
            await process.stdin.drain()
            process.stdin.close()
            await process.wait()
        except OSError:
            pass
        if process.returncode != 0:
            _LOGGER.warning(f"Error passing data for {self.station} {self.profile} {self.start_epoch} {self.end_epoch}, return code {process.returncode}")
            return

        _LOGGER.info(f"Flagged passed data for {self.station} {self.profile} {self.start_epoch} {self.end_epoch}")

    async def is_blocking(self, station: str):
        return self.station == station

    async def wait_for_done(self):
        await self._done.wait()


_queued_updates: typing.List[_PassOperation] = list()
_new_queued_update = asyncio.Event()


async def _process_queue():
    while True:
        await _new_queued_update.wait()
        n_process = len(_queued_updates)
        _new_queued_update.clear()
        for u in list(_queued_updates[0:n_process]):
            await u.update()
        del _queued_updates[0:n_process]


async def _pass_data(start_epoch: int, end_epoch: int, station: str, profile: str, comment: str) -> None:
    _LOGGER.debug(f"Passing data for {station} {profile} {start_epoch} {end_epoch}")
    op = _PassOperation(start_epoch, end_epoch, station, profile, comment)
    await op.apply()
    _queued_updates.append(op)
    _new_queued_update.set()


async def _wait_for_passed(station: str, writer: asyncio.StreamWriter) -> None:
    def is_blocked():
        for u in _queued_updates:
            if not u.is_blocking(station):
                continue
            return u
        return None

    if not is_blocked():
        _LOGGER.debug(f"No active passes on {station} to wait for")
        return

    try:
        writer.write(struct.pack('<B', 1))
        await writer.drain()
    except OSError:
        return

    _LOGGER.debug(f"Waiting for any {station} passes to complete")

    while True:
        blocker = is_blocked()
        if not blocker:
            _LOGGER.debug(f"All passes for {station} completed")
            return
        await blocker.wait_for_done()


async def _connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    _LOGGER.debug("Accepted connection")
    try:
        operation = struct.unpack('<B', await reader.readexactly(1))[0]

        if operation == 1:
            try:
                n = struct.unpack('<I', await reader.readexactly(4))[0]
                station = (await reader.readexactly(n)).decode('utf-8')
            except (OSError, UnicodeDecodeError, EOFError):
                return
            await _wait_for_passed(station, writer)
            return

        try:
            start_epoch = struct.unpack('<Q', await reader.readexactly(8))[0]
            end_epoch = struct.unpack('<Q', await reader.readexactly(8))[0]

            n = struct.unpack('<I', await reader.readexactly(4))[0]
            station = (await reader.readexactly(n)).decode('utf-8')
            n = struct.unpack('<I', await reader.readexactly(4))[0]
            profile = (await reader.readexactly(n)).decode('utf-8')
            n = struct.unpack('<I', await reader.readexactly(4))[0]
            comment = (await reader.readexactly(n)).decode('utf-8')
        except (OSError, UnicodeDecodeError, EOFError):
            return

        if not isfinite(start_epoch) or not isfinite(end_epoch) or end_epoch <= start_epoch:
            return
        if len(station) == 0 or len(profile) == 0:
            return

        await _pass_data(start_epoch, end_epoch, station, profile, comment)
    finally:
        try:
            writer.close()
        except OSError:
            pass


def main():
    parser = argparse.ArgumentParser(description="Forge CPD3 pass control server.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--socket',
                       dest="socket",
                       help="the Unix socket name")
    group.add_argument('--systemd',
                       dest='systemd', action='store_true',
                       help="receive the socket from systemd")

    args = parser.parse_args()
    if args.debug:
        root_logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)-31s %(message)s')
        handler.setFormatter(formatter)
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)

    loop = asyncio.get_event_loop()

    if args.systemd:
        import systemd.daemon
        import socket

        def factory():
            reader = asyncio.StreamReader()
            protocol = asyncio.StreamReaderProtocol(reader, _connection)
            return protocol

        for fd in systemd.daemon.listen_fds():
            _LOGGER.info(f"Binding to systemd socket {fd}")
            sock = socket.socket(fileno=fd, type=socket.SOCK_STREAM, family=socket.AF_UNIX, proto=0)
            loop.create_task(loop.create_server(factory, sock=sock))

        async def heartbeat():
            systemd.daemon.notify("READY=1")
            while True:
                await asyncio.sleep(10)
                systemd.daemon.notify("WATCHDOG=1")

        loop.create_task(heartbeat())
    elif args.socket:
        _LOGGER.info(f"Binding to socket {args.socket}")
        try:
            os.unlink(args.socket)
        except OSError:
            pass
        loop.create_task(asyncio.start_unix_server(_connection, path=args.socket))
    else:
        name = CONFIGURATION.get('CPD3.PASS.SOCKET', '/run/forge-cpd3-pass.socket')
        _LOGGER.info(f"Binding to socket {name}")
        try:
            os.unlink(name)
        except OSError:
            pass
        loop.create_task(asyncio.start_unix_server(_connection, path=name))

    loop.create_task(_process_queue())

    loop.add_signal_handler(signal.SIGINT, loop.stop)
    loop.add_signal_handler(signal.SIGTERM, loop.stop)
    loop.run_forever()


if __name__ == '__main__':
    main()
