import typing
import os
import asyncio
import logging
import struct
from forge.tasks import background_task
from forge.vis import CONFIGURATION
from forge.service import SocketServer
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


async def _prune() -> typing.NoReturn:
    while True:
        await asyncio.sleep(30)
        manager.prune()


class Server(SocketServer):
    DESCRIPTION = "Forge visualization export server."

    async def connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
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

        read_detector = asyncio.ensure_future(_detect_read_closed())
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
        if export_file is None:
            _LOGGER.warning(f"Export failed {station},{mode_name},{export_key},{start_epoch_ms},{end_epoch_ms}")
            try:
                writer.close()
            except OSError:
                pass
            return

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

    @property
    def default_socket(self) -> str:
        return CONFIGURATION.get('EXPORT.SOCKET', '/run/forge-vis-export.socket')


def main():
    asyncio.set_event_loop(asyncio.new_event_loop())
    server = Server()
    background_task(_prune())
    server.run()


if __name__ == '__main__':
    main()
