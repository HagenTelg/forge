import typing
import os
import asyncio
import logging
import argparse
import signal
from abc import ABC, abstractmethod
from forge.tasks import background_task

_LOGGER = logging.getLogger(__name__)


class UnixServer(ABC):
    DESCRIPTION = ""

    def __init__(self):
        parser = argparse.ArgumentParser(description=self.DESCRIPTION)

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

        self.add_arguments(parser)
        self.args = parser.parse_args()
        if self.args.debug:
            root_logger = logging.getLogger()
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(name)-40s %(message)s')
            handler.setFormatter(formatter)
            root_logger.setLevel(logging.DEBUG)
            root_logger.addHandler(handler)

        loop = asyncio.get_event_loop()

        if self.args.systemd:
            import systemd.daemon
            import socket

            def factory():
                reader = asyncio.StreamReader()
                protocol = asyncio.StreamReaderProtocol(reader, self.connection)
                return protocol

            for fd in systemd.daemon.listen_fds():
                _LOGGER.info(f"Binding to systemd socket {fd}")
                sock = socket.socket(fileno=fd, type=socket.SOCK_STREAM, family=socket.AF_UNIX, proto=0)
                background_task(loop.create_server(factory, sock=sock))

            async def heartbeat():
                systemd.daemon.notify("READY=1")
                while True:
                    await asyncio.sleep(10)
                    systemd.daemon.notify("WATCHDOG=1")

            background_task(heartbeat())
        elif self.args.socket:
            _LOGGER.info(f"Binding to socket {self.args.socket}")
            try:
                os.unlink(self.args.socket)
            except OSError:
                pass
            background_task(asyncio.start_unix_server(self.connection, path=self.args.socket))
        else:
            name = self.default_socket
            _LOGGER.info(f"Binding to socket {name}")
            try:
                os.unlink(name)
            except OSError:
                pass
            background_task(asyncio.start_unix_server(self.connection, path=name))

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        pass

    @property
    def default_socket(self) -> str:
        return ''

    @abstractmethod
    async def connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        pass

    @staticmethod
    def run() -> None:
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, loop.stop)
        loop.add_signal_handler(signal.SIGTERM, loop.stop)
        loop.run_forever()
