import typing
import asyncio
import logging
from forge.service import SocketServer
from forge.archive import CONFIGURATION
from .control import Controller
from .diagnostics import Diagnostics

_LOGGER = logging.getLogger(__name__)


control: Controller = None
diagnostics: Diagnostics = None


class Server(SocketServer):
    DESCRIPTION = "Forge archive controller."

    async def connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await control.connection(reader, writer)
        except:
            _LOGGER.debug("Error in connection", exc_info=True)
        finally:
            try:
                writer.close()
            except OSError:
                pass

    async def initialize(self) -> None:
        global control
        _LOGGER.debug("Initializing controller")
        control = Controller()
        await control.initialize()

        diagnostic_socket = CONFIGURATION.get('ARCHIVE.DIAGNOSTIC.SOCKET', CONFIGURATION.get('ARCHIVE.DIAGNOSTIC_SOCKET'))
        if diagnostic_socket:
            _LOGGER.debug("Initializing diagnostics")
            global diagnostics
            diagnostics = Diagnostics(diagnostic_socket, control, CONFIGURATION.get('ARCHIVE.DIAGNOSTIC.MODE'))
            await diagnostics.initialize()

    @property
    def default_socket(self) -> str:
        return CONFIGURATION.get('ARCHIVE.SOCKET', '/run/forge-archive.socket')


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    server = Server()
    server.run()
    if diagnostics:
        _LOGGER.debug("Shutting down diagnostics")
        diagnostics.shutdown()
    _LOGGER.debug("Shutting down controller")
    control.shutdown()


if __name__ == '__main__':
    main()
