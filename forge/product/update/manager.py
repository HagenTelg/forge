import typing
import asyncio
import logging
import time
import re
import os
from abc import ABC, abstractmethod
from forge.tasks import wait_cancelable
from forge.range import intersects
from forge.archive.client import data_notification_key
from forge.archive.client.connection import Connection
from forge.dashboard.report import report_ok, report_failed
from .tracker import FileModifiedTracker

_LOGGER = logging.getLogger(__name__)


class UpdateController(ABC):
    UPDATER_DESCRIPTION = ""
    UPDATER_CONNECTION_NAME = ""
    CONTROL_DESCRIPTION = ""

    CANDIDATE_PROCESS_DELAY: float = 10.0
    AUTOMATIC_COMMIT: bool = False
    SYNCHRONOUS_NOTIFICATION: bool = False

    def __init__(self, connection: Connection):
        self.connection = connection
        self._lock: asyncio.Lock = None
        self._updated: asyncio.Event = None
        self._trackers: typing.List[FileModifiedTracker] = list()

        self._notification_queue: typing.List[typing.Tuple[FileModifiedTracker, int, int]] = list()
        self._next_candidate_process: typing.Optional[float] = time.monotonic() + self.CANDIDATE_PROCESS_DELAY

        self._external_update_queue: typing.List[typing.Tuple[FileModifiedTracker, int, int]] = list()
        self._external_commit_queue: typing.List[typing.Tuple[FileModifiedTracker, int, int]] = list()

        self._commit_request: typing.Set[FileModifiedTracker] = set()
        self._suspended_trackers: typing.Set[FileModifiedTracker] = set()

    @abstractmethod
    async def create_trackers(self) -> typing.List[FileModifiedTracker]:
        pass

    async def _tracker_notified(self, key: str, start_epoch_ms: int, end_epoch_ms: int,
                                tracker: FileModifiedTracker) -> None:
        async with self._lock:
            if tracker in self._suspended_trackers:
                _LOGGER.debug(f"Archive notification {start_epoch_ms},{end_epoch_ms} ignored on suspended {str(tracker)}")
                return
            self._notification_queue.append((tracker, start_epoch_ms, end_epoch_ms))
        self._updated.set()
        _LOGGER.debug(f"Archive notification {start_epoch_ms},{end_epoch_ms} received for {str(tracker)}")

    async def initialize(self) -> None:
        self._lock = asyncio.Lock()
        self._updated = asyncio.Event()
        self._trackers = await self.create_trackers()
        _LOGGER.debug(f"Update controller starting with {len(self._trackers)} trackers")

        for tracker in self._trackers:
            tracker.load_state()
        for tracker in self._trackers:
            await self.connection.listen_notification(
                data_notification_key(tracker.station, tracker.archive),
                self._tracker_notified, tracker,
                synchronous=self.SYNCHRONOUS_NOTIFICATION,
            )

    async def _matched_trackers(
            self,
            station: typing.Optional[str] = None,
            archive: typing.Optional[str] = None,
            key: typing.Optional[str] = None,
    ) -> typing.AsyncIterable[FileModifiedTracker]:
        if key:
            try:
                key = re.compile(key, flags=re.IGNORECASE)
            except re.error:
                _LOGGER.error("Invalid notify key selection", exc_info=True)
                return
        else:
            key = None
        async with self._lock:
            for tracker in self._trackers:
                if station and tracker.station != station:
                    continue
                if archive and tracker.archive != archive:
                    continue
                if key and not key.fullmatch(tracker.update_key or ""):
                    continue
                yield tracker

    async def notify_rescan(
            self,
            start_epoch_ms: int, end_epoch_ms: int,
            station: typing.Optional[str] = None,
            archive: typing.Optional[str] = None,
            key: typing.Optional[str] = None,
    ) -> None:
        async for tracker in self._matched_trackers(station, archive, key):
            self._notification_queue.append((tracker, start_epoch_ms, end_epoch_ms))
            self._updated.set()
            _LOGGER.debug(f"Rescan on {start_epoch_ms},{end_epoch_ms} sent to {str(tracker)}")

    async def notify_update(
            self,
            start_epoch_ms: int, end_epoch_ms: int,
            station: typing.Optional[str] = None,
            archive: typing.Optional[str] = None,
            key: typing.Optional[str] = None,
    ) -> None:
        async for tracker in self._matched_trackers(station, archive, key):
            self._external_update_queue.append((tracker, start_epoch_ms, end_epoch_ms))
            self._updated.set()
            _LOGGER.debug(f"Explicit update {start_epoch_ms},{end_epoch_ms} sent to {str(tracker)}")

    async def notify_external_commit(
            self,
            start_epoch_ms: int, end_epoch_ms: int,
            station: typing.Optional[str] = None,
            archive: typing.Optional[str] = None,
            key: typing.Optional[str] = None,
    ) -> None:
        async for tracker in self._matched_trackers(station, archive, key):
            self._external_commit_queue.append((tracker, start_epoch_ms, end_epoch_ms))
            self._updated.set()
            _LOGGER.debug(f"External commit {start_epoch_ms},{end_epoch_ms} sent to {str(tracker)}")

    async def commit(
            self,
            station: typing.Optional[str] = None,
            archive: typing.Optional[str] = None,
            key: typing.Optional[str] = None,
    ) -> None:
        async for tracker in self._matched_trackers(station, archive, key):
            self._commit_request.add(tracker)
            if self._next_candidate_process is not None:
                self._next_candidate_process = time.monotonic()
            self._updated.set()
            _LOGGER.debug(f"Commit requested for {str(tracker)}")

    async def suspend_tracker(
            self,
            station: typing.Optional[str] = None,
            archive: typing.Optional[str] = None,
            key: typing.Optional[str] = None,
    ) -> None:
        async for tracker in self._matched_trackers(station, archive, key):
            self._suspended_trackers.add(tracker)
            _LOGGER.debug(f"Suspend set for {str(tracker)}")

    async def unsuspend_tracker(
            self,
            station: typing.Optional[str] = None,
            archive: typing.Optional[str] = None,
            key: typing.Optional[str] = None,
    ) -> None:
        async for tracker in self._matched_trackers(station, archive, key):
            self._suspended_trackers.discard(tracker)
            _LOGGER.debug(f"Suspend cleared for {str(tracker)}")

    async def discard(
            self,
            start_epoch_ms: int, end_epoch_ms: int,
            discard_outputs: bool = False,
            station: typing.Optional[str] = None,
            archive: typing.Optional[str] = None,
            key: typing.Optional[str] = None,
    ) -> None:
        async for tracker in self._matched_trackers(station, archive, key):
            for idx in reversed(range(len(self._notification_queue))):
                check_tracker, check_start, check_end = self._notification_queue[idx]
                if check_tracker != tracker:
                    continue
                if not intersects(check_start, check_end, start_epoch_ms, end_epoch_ms):
                    continue
                del self._notification_queue[idx]
                _LOGGER.debug(f"Removed notification on {check_start},{check_end} for {str(check_tracker)}")
            tracker.discard_updates(start_epoch_ms, end_epoch_ms)
            if discard_outputs:
                tracker.discard_outputs(start_epoch_ms, end_epoch_ms)
            tracker.save_state()

    async def _flush_notifications(self, save_state: bool = True) -> None:
        async with self._lock:
            to_notify = list(self._notification_queue)
            self._notification_queue.clear()
        for tracker, start_epoch_ms, end_epoch_ms in to_notify:
            _LOGGER.debug(f"Notifying candidate {start_epoch_ms},{end_epoch_ms} to {str(tracker)}")
            tracker.notify_candidate(start_epoch_ms, end_epoch_ms, save_state=save_state)
        if to_notify and self._next_candidate_process is None:
            self._next_candidate_process = time.monotonic() + self.CANDIDATE_PROCESS_DELAY

    async def _flush_external_update(self, save_state: bool = True) -> bool:
        async with self._lock:
            to_notify = list(self._external_update_queue)
            self._external_update_queue.clear()
        if not to_notify:
            return False
        for tracker, start_epoch_ms, end_epoch_ms in to_notify:
            _LOGGER.debug(f"Notifying external update {start_epoch_ms},{end_epoch_ms} to {str(tracker)}")
            await tracker.notify_update(start_epoch_ms, end_epoch_ms, save_state=save_state)
        return True

    async def _flush_external_commit(self, save_state: bool = True) -> None:
        async with self._lock:
            to_notify = list(self._external_commit_queue)
            self._external_commit_queue.clear()
        for tracker, start_epoch_ms, end_epoch_ms in to_notify:
            _LOGGER.debug(f"Notifying external commit {start_epoch_ms},{end_epoch_ms} to {str(tracker)}")
            tracker.notify_external_commit(start_epoch_ms, end_epoch_ms, save_state=save_state)

    async def _process_candidates(self) -> bool:
        if self._next_candidate_process is None:
            return False
        if self._next_candidate_process > time.monotonic():
            return False
        _LOGGER.debug("Processing pending candidates")
        self._next_candidate_process = None
        any_updates = False
        for tracker in self._trackers:
            if await tracker.process_candidates():
                any_updates = True
        return any_updates

    async def _process_commit(self) -> None:
        async with self._lock:
            to_commit = list(self._commit_request)
            self._commit_request.clear()
        if not to_commit:
            return

        _LOGGER.debug(f"Processing {len(to_commit)} pending commits")
        for tracker in to_commit:
            await tracker.commit()

    async def run(self, before_idle: typing.Optional[typing.Callable[[], typing.Awaitable]] = None) -> typing.NoReturn:
        while True:
            self._updated.clear()

            await self._flush_notifications()
            any_updated = await self._flush_external_update()
            await self._flush_external_commit()
            any_updated = await self._process_candidates() or any_updated
            if any_updated and self.AUTOMATIC_COMMIT:
                async with self._lock:
                    self._commit_request.clear()
                for tracker in self._trackers:
                    await tracker.commit()
            else:
                await self._process_commit()

            if not before_idle:
                wait_time = None
            else:
                wait_time = await before_idle()

            if self._next_candidate_process is not None:
                if wait_time is None:
                    wait_time = self._next_candidate_process - time.monotonic()
                else:
                    wait_time = min(wait_time, self._next_candidate_process - time.monotonic())

            if wait_time is None:
                await self._updated.wait()
                continue

            wait_time = max(wait_time, 0.001)
            try:
                await wait_cancelable(self._updated.wait(), wait_time)
            except asyncio.TimeoutError:
                pass

    async def shutdown(self) -> None:
        await self._flush_notifications(save_state=False)
        await self._flush_external_update(save_state=False)
        await self._flush_external_commit(save_state=False)
        for tracker in self._trackers:
            tracker.save_state(sync=True)

    @classmethod
    def create_updater(cls, connection: Connection, args) -> "UpdateController":
        return cls(connection)

    @classmethod
    def updater_control_socket(cls) -> typing.Optional[str]:
        raise NotImplementedError

    @classmethod
    def add_updater_arguments(cls, parser) -> None:
        pass

    @classmethod
    def run_updater(cls) -> None:
        import argparse
        import signal
        import struct
        from forge.product.update import CONFIGURATION

        parser = argparse.ArgumentParser(description=cls.UPDATER_DESCRIPTION)

        parser.add_argument('--debug',
                            dest='debug', action='store_true',
                            help="enable debug output")
        parser.add_argument('--systemd',
                            dest='systemd', action='store_true',
                            help="enable systemd integration")
        parser.add_argument('--dashboard',
                            dest='dashboard', type=str,
                            help="dashboard notification code")
        group = parser.add_mutually_exclusive_group()
        group.add_argument('--server-host',
                           dest='tcp_server',
                           help="archive server host")
        group.add_argument('--server-socket',
                           dest='unix_socket',
                           help="archive server Unix socket")
        parser.add_argument('--server-port',
                            dest='tcp_port',
                            type=int,
                            default=CONFIGURATION.get("ARCHIVE.PORT"),
                            help="archive server port")

        parser.add_argument('--control-socket',
                            dest='control_socket', default=cls.updater_control_socket(),
                            help="override destination station")

        cls.add_updater_arguments(parser)

        args = parser.parse_args()
        if args.tcp_server and not args.tcp_port:
            parser.error("Both a server host and port must be specified")

        if args.debug:
            from forge.log import set_debug_logger
            set_debug_logger()

        loop = asyncio.new_event_loop()

        connection: Connection = None
        controller: UpdateController = None
        control_server: "asyncio.Server" = None

        async def control_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            async def read_string() -> str:
                n = struct.unpack('<I', await reader.readexactly(4))[0]
                return (await reader.readexactly(n)).decode('utf-8')

            _LOGGER.debug("Control connection accepted")
            try:
                command = struct.unpack('<B', await reader.readexactly(1))[0]
                if command == 0:
                    start, end = struct.unpack('<qq', await reader.readexactly(16))
                    station = await read_string()
                    archive = await read_string()
                    key = await read_string()
                    if controller:
                        await controller.notify_update(start, end, station, archive, key)
                        writer.write(struct.pack('<B', 0))
                    else:
                        writer.write(struct.pack('<B', 1))
                    await writer.drain()
                elif command == 1:
                    station = await read_string()
                    archive = await read_string()
                    key = await read_string()
                    if controller:
                        await controller.commit(station, archive, key)
                        writer.write(struct.pack('<B', 0))
                    else:
                        writer.write(struct.pack('<B', 1))
                    await writer.drain()
                elif command == 2:
                    start, end = struct.unpack('<qq', await reader.readexactly(16))
                    station = await read_string()
                    archive = await read_string()
                    key = await read_string()
                    if controller:
                        await controller.notify_external_commit(start, end, station, archive, key)
                        writer.write(struct.pack('<B', 0))
                    else:
                        writer.write(struct.pack('<B', 1))
                    await writer.drain()
                elif command == 3:
                    start, end = struct.unpack('<qq', await reader.readexactly(16))
                    station = await read_string()
                    archive = await read_string()
                    key = await read_string()
                    if controller:
                        await controller.notify_rescan(start, end, station, archive, key)
                        writer.write(struct.pack('<B', 0))
                    else:
                        writer.write(struct.pack('<B', 1))
                    await writer.drain()
                elif command == 4:
                    station = await read_string()
                    archive = await read_string()
                    key = await read_string()
                    if controller:
                        await controller.suspend_tracker(station, archive, key)
                        writer.write(struct.pack('<B', 0))
                    else:
                        writer.write(struct.pack('<B', 1))
                    await writer.drain()
                elif command == 5:
                    station = await read_string()
                    archive = await read_string()
                    key = await read_string()
                    if controller:
                        await controller.unsuspend_tracker(station, archive, key)
                        writer.write(struct.pack('<B', 0))
                    else:
                        writer.write(struct.pack('<B', 1))
                    await writer.drain()
                elif command == 6:
                    start, end, discard_outputs = struct.unpack('<qqB', await reader.readexactly(17))
                    station = await read_string()
                    archive = await read_string()
                    key = await read_string()
                    if controller:
                        await controller.discard(start, end, discard_outputs, station, archive, key)
                        writer.write(struct.pack('<B', 0))
                    else:
                        writer.write(struct.pack('<B', 1))
                    await writer.drain()
                else:
                    _LOGGER.error(f"Invalid updater command {command}")
            finally:
                try:
                    writer.close()
                except:
                    pass
                _LOGGER.debug("Control connection closed")

        async def initialize():
            nonlocal connection
            nonlocal controller
            nonlocal control_server

            if args.tcp_server and args.tcp_port:
                _LOGGER.debug(f"Connecting to archive TCP socket {args.tcp_server}:{args.tcp_port}")
                reader, writer = await asyncio.open_connection(args.tcp_server, int(args.tcp_port))
                connection = Connection(reader, writer, cls.UPDATER_CONNECTION_NAME)
            elif args.unix_socket:
                _LOGGER.debug(f"Connecting to archive Unix socket {args.unix_socket}")
                reader, writer = await asyncio.open_unix_connection(args.unix_socket)
                connection = Connection(reader, writer, cls.UPDATER_CONNECTION_NAME)
            else:
                connection = await Connection.default_connection(cls.UPDATER_CONNECTION_NAME)

            await connection.startup()

            _LOGGER.debug("Initializing update controller")
            controller = cls.create_updater(connection, args)

            if args.systemd:
                import systemd.daemon
                _LOGGER.debug("Started startup keepalive")

                async def send_keepalive() -> None:
                    while True:
                        await asyncio.sleep(10)
                        systemd.daemon.notify("EXTEND_TIMEOUT_USEC=30000000")
                        _LOGGER.debug("Startup keepalive sent")

                keepalive = loop.create_task(send_keepalive())
            else:
                keepalive = None

            await controller.initialize()

            if keepalive:
                _LOGGER.debug("Shutting down startup keepalive")
                t = keepalive
                keepalive = None
                try:
                    t.cancel()
                except:
                    pass
                try:
                    await t
                except:
                    pass

            control_socket = args.control_socket
            if control_socket:
                _LOGGER.info(f"Binding to external control socket {control_socket}")
                try:
                    os.unlink(control_socket)
                except OSError:
                    pass
                control_server = await asyncio.start_unix_server(control_connection, path=control_socket)

        async def shutdown():
            nonlocal connection
            nonlocal controller
            nonlocal control_server

            if control_server:
                _LOGGER.debug("Shutting down external control socket")
                try:
                    control_server.close()
                except:
                    pass
                try:
                    await wait_cancelable(control_server.wait_closed(), 2.0)
                except:
                    pass
            control_server = None

            _LOGGER.debug("Shutting down update controller")
            do_shutdown = controller
            controller = None
            await do_shutdown.shutdown()

            _LOGGER.debug("Shutting down archive connection")
            do_shutdown = connection
            connection = None
            await do_shutdown.shutdown()

        loop.run_until_complete(initialize())

        dashboard: typing.Optional[typing.Callable[[], typing.Awaitable]] = None
        if args.dashboard:
            last_notification: typing.Optional[float] = None

            async def notify_dashboard() -> float:
                nonlocal last_notification

                now = time.monotonic()
                if not last_notification or (now - last_notification) >= 300:
                    last_notification = now
                    await report_ok(args.dashboard)
                return 300 - (now - last_notification)

            dashboard = notify_dashboard

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

        _LOGGER.info("Update controller ready")

        controller_run = loop.create_task(controller.run(before_idle=dashboard))
        loop.add_signal_handler(signal.SIGINT, controller_run.cancel)
        loop.add_signal_handler(signal.SIGTERM, controller_run.cancel)
        try:
            loop.run_until_complete(controller_run)
        except asyncio.CancelledError:
            pass
        except:
            if args.dashboard:
                loop.run_until_complete(report_failed(args.dashboard, exc_info=True))
            raise

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

        loop.run_until_complete(shutdown())
        loop.close()

    @classmethod
    def run_control(cls) -> None:
        import argparse
        import struct
        from math import floor, ceil
        from forge.const import STATIONS
        from forge.timeparse import parse_time_bounds_arguments

        parser = argparse.ArgumentParser(description=cls.CONTROL_DESCRIPTION)

        parser.add_argument('--debug',
                            dest='debug', action='store_true',
                            help="enable debug output")
        parser.add_argument('--dashboard',
                            dest='dashboard', type=str,
                            help="dashboard notification code")
        parser.add_argument('--control-socket',
                            dest='control_socket', default=cls.updater_control_socket(),
                            help="override destination station")

        subparsers = parser.add_subparsers(dest='command')

        command_parser = subparsers.add_parser('update',
                                               help="queue an update to be processed")
        command_parser.add_argument('--multiple',
                                    dest='multiple', action='store_true',
                                    help="required if a station is not selected")
        command_parser.add_argument('--station',
                                    dest='station',
                                    help="limit updates to a station")
        command_parser.add_argument('--archive',
                                    dest='archive',
                                    choices=["raw", "edited", "clean", "avgh", "avgd", "avgm"],
                                    help="limit updates to an archive")
        command_parser.add_argument('--key',
                                    dest='key',
                                    help="limit updates to a key matching a regular expression")
        command_parser.add_argument('time', help="time bounds to update", nargs='+')

        command_parser = subparsers.add_parser('run',
                                               help="run pending updates")
        command_parser.add_argument('--multiple',
                                    dest='multiple', action='store_true',
                                    help="required if a station is not selected")
        command_parser.add_argument('--station',
                                    dest='station',
                                    help="limit updates to a station")
        command_parser.add_argument('--archive',
                                    dest='archive',
                                    choices=["raw", "edited", "clean", "avgh", "avgd", "avgm"],
                                    help="limit updates to an archive")
        command_parser.add_argument('--key',
                                    dest='key',
                                    help="limit updates to a key matching a regular expression")

        command_parser = subparsers.add_parser('external',
                                               help="notify of external completion of updates")
        command_parser.add_argument('--multiple',
                                    dest='multiple', action='store_true',
                                    help="required if a station is not selected")
        command_parser.add_argument('--station',
                                    dest='station',
                                    help="limit updates to a station")
        command_parser.add_argument('--archive',
                                    dest='archive',
                                    choices=["raw", "edited", "clean", "avgh", "avgd", "avgm"],
                                    help="limit updates to an archive")
        command_parser.add_argument('--key',
                                    dest='key',
                                    help="limit updates to a key matching a regular expression")
        command_parser.add_argument('time', help="time bounds to notify", nargs='+')

        command_parser = subparsers.add_parser('rescan',
                                               help="rescan input files for modifications")
        command_parser.add_argument('--multiple',
                                    dest='multiple', action='store_true',
                                    help="required if a station is not selected")
        command_parser.add_argument('--station',
                                    dest='station',
                                    help="limit rescan to a station")
        command_parser.add_argument('--archive',
                                    dest='archive',
                                    choices=["raw", "edited", "clean", "avgh", "avgd", "avgm"],
                                    help="limit rescan to an archive")
        command_parser.add_argument('--key',
                                    dest='key',
                                    help="limit rescan to a key matching a regular expression")
        command_parser.add_argument('time', help="time bounds to rescan", nargs='+')

        command_parser = subparsers.add_parser('suspend',
                                               help="suspend automatic file modification tracking")
        command_parser.add_argument('--multiple',
                                    dest='multiple', action='store_true',
                                    help="required if a station is not selected")
        command_parser.add_argument('--station',
                                    dest='station',
                                    help="limit suspend to a station")
        command_parser.add_argument('--archive',
                                    dest='archive',
                                    choices=["raw", "edited", "clean", "avgh", "avgd", "avgm"],
                                    help="limit suspend to an archive")
        command_parser.add_argument('--key',
                                    dest='key',
                                    help="limit suspend to a key matching a regular expression")

        command_parser = subparsers.add_parser('unsuspend',
                                               help="unsuspend automatic file modification tracking")
        command_parser.add_argument('--multiple',
                                    dest='multiple', action='store_true',
                                    help="required if a station is not selected")
        command_parser.add_argument('--station',
                                    dest='station',
                                    help="limit unsuspend to a station")
        command_parser.add_argument('--archive',
                                    dest='archive',
                                    choices=["raw", "edited", "clean", "avgh", "avgd", "avgm"],
                                    help="limit unsuspend to an archive")
        command_parser.add_argument('--key',
                                    dest='key',
                                    help="limit unsuspend to a key matching a regular expression")

        command_parser = subparsers.add_parser('discard',
                                               help="discard updates")
        command_parser.add_argument('--multiple',
                                    dest='multiple', action='store_true',
                                    help="required if a station is not selected")
        command_parser.add_argument('--station',
                                    dest='station',
                                    help="limit discard to a station")
        command_parser.add_argument('--archive',
                                    dest='archive',
                                    choices=["raw", "edited", "clean", "avgh", "avgd", "avgm"],
                                    help="limit discard to an archive")
        command_parser.add_argument('--key',
                                    dest='key',
                                    help="limit discard to a key matching a regular expression")
        command_parser.add_argument('--outputs',
                                    dest='discard_outputs', action='store_true',
                                    help="also discard existing outputs")
        command_parser.add_argument('time', help="time bounds to discard", nargs='+')

        args = parser.parse_args()
        if not args.control_socket:
            parser.error("No control socket available")
        if not args.command:
            parser.error("No command specified")

        if args.debug:
            from forge.log import set_debug_logger
            set_debug_logger()

        station = args.station
        if station:
            station = station.lower()
            if station not in STATIONS:
                parser.error("Invalid station code")
        archive = args.archive
        key = args.key
        if key:
            try:
                re.compile(key, flags=re.IGNORECASE)
            except re.error:
                parser.error("Invalid key")
        else:
            key = None

        if not station and not args.multiple:
            parser.error("--multiple required when --station is not specified")

        loop = asyncio.new_event_loop()

        async def run():
            _LOGGER.debug(f"Connecting to control socket {args.control_socket}")
            reader, writer = await asyncio.open_unix_connection(args.control_socket)

            def write_string(s: str):
                raw = s.encode('utf-8')
                writer.write(struct.pack('<I', len(raw)))
                writer.write(raw)

            if args.command == 'update':
                start, end = parse_time_bounds_arguments(args.time)
                start = start.timestamp()
                end = end.timestamp()

                writer.write(struct.pack('<Bqq', 0, int(floor(start * 1000)), int(ceil(end * 1000))))
                write_string(station or "")
                write_string(archive or "")
                write_string(key or "")
            elif args.command == 'run':
                writer.write(struct.pack('<B', 1))
                write_string(station or "")
                write_string(archive or "")
                write_string(key or "")
            elif args.command == 'external':
                start, end = parse_time_bounds_arguments(args.time)
                start = start.timestamp()
                end = end.timestamp()

                writer.write(struct.pack('<Bqq', 2, int(floor(start * 1000)), int(ceil(end * 1000))))
                write_string(station or "")
                write_string(archive or "")
                write_string(key or "")
            elif args.command == 'rescan':
                start, end = parse_time_bounds_arguments(args.time)
                start = start.timestamp()
                end = end.timestamp()

                writer.write(struct.pack('<Bqq', 3, int(floor(start * 1000)), int(ceil(end * 1000))))
                write_string(station or "")
                write_string(archive or "")
                write_string(key or "")
            elif args.command == 'suspend':
                writer.write(struct.pack('<B', 4))
                write_string(station or "")
                write_string(archive or "")
                write_string(key or "")
            elif args.command == 'unsuspend':
                writer.write(struct.pack('<B', 5))
                write_string(station or "")
                write_string(archive or "")
                write_string(key or "")
            elif args.command == 'discard':
                start, end = parse_time_bounds_arguments(args.time)
                start = start.timestamp()
                end = end.timestamp()

                writer.write(struct.pack('<BqqB', 6, int(floor(start * 1000)), int(ceil(end * 1000)),
                                         1 if args.discard_outputs else 0))
                write_string(station or "")
                write_string(archive or "")
                write_string(key or "")

            result = struct.unpack('<B', await reader.readexactly(1))[0]

            writer.close()
            return result

        try:
            result = loop.run_until_complete(run())
        except:
            if args.dashboard:
                loop.run_until_complete(report_failed(args.dashboard, exc_info=True))
            raise

        if result != 0:
            print("Error sending command")

            if args.dashboard:
                loop.run_until_complete(report_failed(args.dashboard, notifications=[{
                    "code": "",
                    "severity": "error",
                    "data": "Error sending update control command."
                }]))
        else:
            if args.dashboard:
                loop.run_until_complete(report_ok(args.dashboard))

        loop.close()
        exit(result)