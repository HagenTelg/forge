import typing
import asyncio
import logging
from forge.service import UnixServer
from forge.acquisition import CONFIGURATION
from ..protocol import deserialize_string
from .dispatch import Dispatch


_LOGGER = logging.getLogger(__name__)


dispatch: Dispatch = None


class Server(UnixServer):
    DESCRIPTION = "Forge acquisition bus."

    async def connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            source = await deserialize_string(reader)
            await dispatch.connection(source, reader, writer)
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
        return CONFIGURATION.get('ACQUISITION.BUS', '/run/forge-acquisition-bus.socket')


def main():
    global dispatch
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    dispatch = Dispatch()
    server = Server()
    server.run()


if __name__ == '__main__':
    main()
