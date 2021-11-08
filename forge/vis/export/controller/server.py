import typing
import os
import asyncio
import logging
import argparse
import struct
import signal
from forge.vis import CONFIGURATION
from .manager import Manager, ExportedFile


_LOGGER = logging.getLogger(__name__)


manager = Manager()


async def _stream_file(writer: asyncio.StreamWriter, export_file: ExportedFile) -> None:
    async def reader():
        try:
            source = os.dup(export_file.file.fileno())
        except OSError:
            _LOGGER.debug("Error initializing file", exc_info=True)
            return

        try:
            offset = 0
            while True:
                data = os.pread(source, 65536, offset)
                if not data:
                    break
                offset += len(data)
                yield data
        finally:
            try:
                os.close(source)
            except OSError:
                pass

    async for chunk in reader():
        try:
            writer.write(chunk)
            await writer.drain()
        except OSError:
            pass


async def _connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    _LOGGER.debug("Accepted connection")

    async def string_arg() -> str:
        arg_len = struct.unpack('<I', await reader.readexactly(4))[0]
        return (await reader.readexactly(arg_len)).decode('utf-8')

    try:
        station = await string_arg()
        mode_name = await string_arg()
        export_key = await string_arg()
        start_epoch_ms = struct.unpack('<q', await reader.readexactly(8))[0]
        end_epoch_ms = struct.unpack('<q', await reader.readexactly(8))[0]
        command = struct.unpack('<B', await reader.readexactly(1))[0]
    except (OSError, UnicodeDecodeError, EOFError):
        try:
            writer.close()
        except OSError:
            pass
        return
    _LOGGER.debug(f"Export request received {station},{mode_name},{export_key},{start_epoch_ms},{end_epoch_ms}")

    result = manager(station, mode_name, export_key, start_epoch_ms, end_epoch_ms)

    async def _detect_read_closed():
        while True:
            try:
                data = await reader.read(1024)
                if not data:
                    break
            except asyncio.CancelledError:
                return
            except (OSError, EOFError):
                break
        result.cancel()

    read_detector = asyncio.get_event_loop().create_task(_detect_read_closed())
    try:
        export_file = await result
    except asyncio.CancelledError:
        try:
            writer.close()
        except OSError:
            pass
        return
    try:
        read_detector.cancel()
        await read_detector
    except:
        pass

    header = struct.pack('<Q', export_file.size)

    def header_string(add: str) -> None:
        nonlocal header
        raw = add.encode('utf-8')
        header += struct.pack('<I', len(raw))
        header += raw

    header_string(export_file.client_name)
    header_string(export_file.media_type)

    writer.write(header)

    if command == 1:
        _LOGGER.debug(f"Export ready for {station},{mode_name},{export_key},{start_epoch_ms},{end_epoch_ms}")
        await writer.drain()
        try:
            writer.close()
        except OSError:
            pass
        return

    _LOGGER.debug(f"Export streaming for {station},{mode_name},{export_key},{start_epoch_ms},{end_epoch_ms}")
    await _stream_file(writer, export_file)
    try:
        writer.close()
    except OSError:
        pass


def main():
    parser = argparse.ArgumentParser(description="Forge visualization export server.")

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
        formatter = logging.Formatter('%(name)-40s %(message)s')
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
        name = CONFIGURATION.get('EXPORT.SOCKET', '/run/forge-vis-export.socket')
        _LOGGER.info(f"Binding to socket {name}")
        try:
            os.unlink(name)
        except OSError:
            pass
        loop.create_task(asyncio.start_unix_server(_connection, path=name))

    async def prune():
        while True:
            await asyncio.sleep(30)
            manager.prune()

    loop.create_task(prune())

    loop.add_signal_handler(signal.SIGINT, loop.stop)
    loop.add_signal_handler(signal.SIGTERM, loop.stop)
    loop.run_forever()


if __name__ == '__main__':
    main()
