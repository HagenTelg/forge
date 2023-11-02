import typing
import os
import sys
import asyncio
import logging
import argparse
import signal
from abc import ABC, abstractmethod
from forge.tasks import background_task

_LOGGER = logging.getLogger(__name__)


class SocketServer(ABC):
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
        group.add_argument('--tcp',
                           dest="tcp", type=int,
                           help="the TCP port to bind")
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
        loop.run_until_complete(self.initialize())

        if self.args.systemd:
            import systemd.daemon
            import socket

            class SystemdStreamReader(asyncio.StreamReader):
                def __init__(self, systemd: str, *args, **kwargs):
                    super().__init__(*args, **kwargs)
                    self.systemd_name = systemd

                def __repr__(self) -> str:
                    return f"StreamReader(systemd={self.systemd_name})"

                def __str__(self) -> str:
                    return self.systemd_name or super().__str__()

            def attached_factory(name: str):
                def factory():
                    reader = SystemdStreamReader(name)
                    protocol = asyncio.StreamReaderProtocol(reader, self.connection)
                    return protocol
                return factory

            def attach_socket(fd):
                if sys.version_info[:2] >= (3, 7):
                    return socket.socket(fileno=fd)

                sock_family = socket.AF_UNIX
                sock_type = socket.SOCK_STREAM
                sock_proto = 0
                temp_socket = socket.socket(fileno=fd, type=sock_type, family=sock_family, proto=sock_proto)
                try:
                    sock_family = temp_socket.getsockopt(socket.SOL_SOCKET, socket.SO_DOMAIN)
                    sock_type = temp_socket.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE)
                    sock_proto = temp_socket.getsockopt(socket.SOL_SOCKET, socket.SO_PROTOCOL)
                except (AttributeError, IOError):
                    _LOGGER.debug("Failed to read socket information", exc_info=True)
                finally:
                    temp_socket.detach()
                return socket.socket(fileno=fd, type=sock_type, family=sock_family, proto=sock_proto)

            def listen_fd_names():
                try:
                    return systemd.daemon.listen_fds_with_names().items()
                except (AttributeError, OSError):
                    return {fd: "" for fd in systemd.daemon.listen_fds()}.items()

            for fd, name in listen_fd_names().items():
                _LOGGER.info(f"Binding to systemd socket {fd}: {name}")
                sock = attach_socket(fd)
                background_task(loop.create_server(attached_factory(name), sock=sock))

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
        elif self.args.tcp:
            _LOGGER.info(f"Binding to port {self.args.tcp}")
            background_task(asyncio.start_server(self.connection, port=self.args.tcp))
        else:
            sock = self.default_socket
            if not sock:
                raise RuntimeError("no default socket available")
            if isinstance(sock, str):
                _LOGGER.info(f"Binding to socket {sock}")
                try:
                    os.unlink(sock)
                except OSError:
                    pass
                background_task(asyncio.start_unix_server(self.connection, path=sock))
            else:
                sock = int(sock)
                _LOGGER.info(f"Binding to port {sock}")
                background_task(asyncio.start_server(self.connection, port=sock))

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        pass

    @property
    def default_socket(self) -> typing.Union[int, str]:
        return 0

    async def initialize(self) -> None:
        pass

    @abstractmethod
    async def connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        pass

    @staticmethod
    def run() -> None:
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, loop.stop)
        loop.add_signal_handler(signal.SIGTERM, loop.stop)
        loop.run_forever()


try:
    _ = asyncio.BaseEventLoop.sendfile
    _asyncio_sendfile = True
except AttributeError:
    _asyncio_sendfile = False

try:
    _ = os.sendfile

    def _do_sendfile(in_fd: int, out_fd: int, total_size: typing.Optional[int]):
        input_blocking_state = os.get_blocking(in_fd)
        output_blocking_state = os.get_blocking(out_fd)
        try:
            os.set_blocking(in_fd, True)
            os.set_blocking(out_fd, True)

            offset = 0
            if total_size is None:
                remaining = os.fstat(in_fd).st_size
            else:
                remaining = total_size
            while remaining > 0:
                n = os.sendfile(out_fd, in_fd, offset, min(remaining, 16 * 1024 * 1024))
                if n <= 0:
                    break
                offset += n
                remaining -= n

            if remaining > 0:
                _LOGGER.debug(f"Incomplete sendfile, {remaining} bytes remaining")
        finally:
            os.set_blocking(out_fd, output_blocking_state)
            os.set_blocking(in_fd, input_blocking_state)


    from concurrent.futures import ThreadPoolExecutor
    _sendfile_pool = ThreadPoolExecutor(thread_name_prefix="SendfilePool")
except AttributeError:
    _sendfile_pool = None


def get_writer_fileno(writer: asyncio.StreamWriter) -> typing.Optional[int]:
    info = writer.get_extra_info('socket')
    if info:
        return info.fileno()

    info = writer.get_extra_info('pipe')
    if info:
        return info.fileno()

    return None


async def send_file_contents(source: typing.BinaryIO, writer: asyncio.StreamWriter,
                             total_size: typing.Optional[int] = None):
    if _asyncio_sendfile:
        _LOGGER.debug("Sending file with asyncio native implementation")
        try:
            await asyncio.get_event_loop().sendfile(writer.transport, source, count=total_size, fallback=True)
            return
        except (NotImplementedError, RuntimeError):
            # Even with the fallback, this can happen (e.x. Unix pipe transports)
            pass

    if _sendfile_pool:
        out_fd = get_writer_fileno(writer)
        if out_fd is not None:
            in_fd = source.fileno()
            _LOGGER.debug("Sending file with os.sendfile")
            low, high = writer.transport.get_write_buffer_limits()
            writer.transport.set_write_buffer_limits(high=0, low=0)
            try:
                await writer.drain()
                await asyncio.get_event_loop().run_in_executor(
                    _sendfile_pool,
                    _do_sendfile,
                    in_fd, out_fd, total_size
                )
                return
            finally:
                writer.transport.set_write_buffer_limits(high=high, low=low)

    _LOGGER.debug("Sending file with a read loop")
    remaining = total_size
    while remaining is None or remaining > 0:
        chunk_size = 64 * 1024
        if remaining:
            chunk_size = min(chunk_size, remaining)
        data = source.read(chunk_size)
        if not data:
            break
        writer.write(data)
        await writer.drain()
        if remaining:
            remaining -= chunk_size

    if remaining:
        _LOGGER.debug(f"Incomplete send, {remaining} bytes remaining")
