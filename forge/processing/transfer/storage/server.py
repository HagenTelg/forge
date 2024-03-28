import typing
import asyncio
import logging
import struct
from forge.service import SocketServer
from forge.crypto import PublicKey
from forge.processing.transfer import CONFIGURATION
from forge.processing.transfer.storage.protocol import ServerConnectionType, FileType, Compression
from forge.processing.transfer.storage.dispatch import Dispatch

_LOGGER = logging.getLogger(__name__)

_dispatch: typing.Optional[Dispatch] = None


class Server(SocketServer):
    DESCRIPTION = "Forge data transfer storage server."

    async def connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        _LOGGER.debug("Accepted connection")

        async def string_arg() -> str:
            arg_len = struct.unpack('<I', await reader.readexactly(4))[0]
            return (await reader.readexactly(arg_len)).decode('utf-8')

        try:
            connection_type = ServerConnectionType(struct.unpack('<B', await reader.readexactly(1))[0])

            if connection_type == ServerConnectionType.ADD_FILE:
                file_type = FileType(struct.unpack('<B', await reader.readexactly(1))[0])
                key = PublicKey.from_public_bytes(await reader.readexactly(32))
                filename = await string_arg()
                station = await string_arg()
                station = station.lower()
                compression = Compression(struct.unpack('<B', await reader.readexactly(1))[0])

                await _dispatch.add(reader, writer, key, file_type, filename, station, compression)
            elif connection_type == ServerConnectionType.GET_FILES:
                await _dispatch.connection(reader, writer)
                writer = None
            else:
                raise ValueError(f"invalid connection type {connection_type}")
        except:
            _LOGGER.debug("Error in connection", exc_info=True)
        finally:
            if writer:
                try:
                    writer.close()
                except OSError:
                    pass

    @property
    def default_socket(self) -> str:
        return CONFIGURATION.get('PROCESSING.TRANSFER.SOCKET', '/run/forge-transfer-storage.socket')


def main():
    global _dispatch

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _dispatch = Dispatch()
    dispatch_prune = loop.create_task(_dispatch.prune_connections())
    server = Server()
    server.run()
    dispatch_prune.cancel()
    try:
        loop.run_until_complete(dispatch_prune)
    except asyncio.CancelledError:
        pass


if __name__ == '__main__':
    main()
