import typing
import os
import asyncio
import logging
import struct
from forge.tasks import background_task
from forge.vis import CONFIGURATION
from forge.service import UnixServer
from .protocol import ConnectionType
from .manager import Manager


_LOGGER = logging.getLogger(__name__)


manager: Manager = None


async def _prune() -> typing.NoReturn:
    retain_ms = int(CONFIGURATION.get('REALTIME.RETAIN_SECONDS', 24 * 60 * 60) * 1000)
    retain_count = int(CONFIGURATION.get('REALTIME.RETAIN_COUNT', 4000))

    while True:
        await manager.prune(maximum_age_ms=retain_ms, maximum_count=retain_count)
        await asyncio.sleep(600)


class Server(UnixServer):
    DESCRIPTION = "Forge visualization realtime server."

    async def connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        async def string_arg() -> str:
            arg_len = struct.unpack('<I', await reader.readexactly(4))[0]
            return (await reader.readexactly(arg_len)).decode('utf-8')

        try:
            connection_type = ConnectionType(struct.unpack('<B', await reader.readexactly(1))[0])

            if connection_type == ConnectionType.WRITE:
                _LOGGER.debug("Accepted write connection")
                try:
                    while True:
                        station = await string_arg()
                        data_name = await string_arg()
                        await manager.write(station, data_name, reader)
                except EOFError:
                    pass
            elif connection_type == ConnectionType.STREAM:
                station = await string_arg()
                data_name = await string_arg()
                _LOGGER.debug(f"Accepted stream connection for {station} {data_name}")

                stream = asyncio.ensure_future(manager.stream(station, data_name, writer))

                while True:
                    try:
                        data = await reader.read(1024)
                        if not data:
                            stream.cancel()
                            break
                    except asyncio.CancelledError:
                        return
                    except (OSError, EOFError):
                        stream.cancel()
                        break

                try:
                    await stream
                except asyncio.CancelledError:
                    pass
            elif connection_type == ConnectionType.READ:
                station = await string_arg()
                data_name = await string_arg()

                _LOGGER.debug(f"Accepted read connection for {station} {data_name}")
                await manager.read(station, data_name, writer)
            else:
                raise ValueError("Invalid connection type")
        except:
            _LOGGER.debug("Error in connection", exc_info=True)
        finally:
            try:
                writer.close()
            except OSError:
                pass
            return

    @property
    def default_socket(self) -> str:
        return CONFIGURATION.get('REALTIME.SOCKET', '/run/forge-vis-realtime.socket')


def main():
    global manager
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    server = Server()
    manager = Manager(CONFIGURATION.get('REALTIME.STORAGE', '/var/lib/forge-vis-realtime'))
    loop.run_until_complete(manager.load_existing())
    background_task(_prune())
    server.run()


if __name__ == '__main__':
    main()
