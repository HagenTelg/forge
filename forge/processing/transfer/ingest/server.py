import typing
import asyncio
import logging
import struct
import argparse
from pathlib import Path
from forge.const import STATIONS
from forge.service import SocketServer
from forge.crypto import PublicKey
from forge.processing.transfer import CONFIGURATION
from forge.processing.transfer.ingest.controller import Controller

_LOGGER = logging.getLogger(__name__)


control: Controller = None


class Server(SocketServer):
    DESCRIPTION = "Forge acquisition data ingest controller."

    async def connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        _LOGGER.debug("Accepted connection")
        try:
            while True:
                try:
                    command = struct.unpack('<B', await reader.readexactly(1))[0]
                except (IOError, EOFError, asyncio.IncompleteReadError):
                    _LOGGER.debug("Connection closed")
                    break
                if command == 0:
                    name_length = struct.unpack('<I', await reader.readexactly(4))[0]
                    file_name = await reader.readexactly(name_length)
                    file_name = file_name.decode('utf-8')
                    file = Path(file_name)

                    station_length = struct.unpack('<I', await reader.readexactly(4))[0]
                    if not station_length:
                        station = None
                    else:
                        station = await reader.readexactly(station_length)
                        station = station.decode('utf-8')
                        station = station.lower()
                        if station not in STATIONS:
                            raise ValueError(f"Invalid station {station.upper()}")

                    has_key = struct.unpack('<B', await reader.readexactly(1))[0]
                    if has_key:
                        key = PublicKey.from_public_bytes(await reader.readexactly(32))
                    else:
                        key = None

                    result = await control.enqueue(file, station, key)

                    if result:
                        writer.write(struct.pack('<B', 0))
                    else:
                        writer.write(struct.pack('<B', 1))
                    await writer.drain()
                else:
                    raise ValueError("Invalid command")
        except:
            _LOGGER.debug("Error in connection", exc_info=True)
        finally:
            try:
                writer.close()
            except OSError:
                pass

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument('--multi-process',
                            dest='multi_process', action='store_true',
                            help="disable single process mode even with debug on")

    async def initialize(self) -> None:
        global control
        _LOGGER.debug("Initializing controller")

        if self.args.multi_process:
            single_process = False
        else:
            single_process = self.args.debug

        control = Controller(single_process)

    @property
    def default_socket(self) -> str:
        return CONFIGURATION.get('PROCESSING.TRANSFER.DATA.SOCKET', '/run/forge-transfer-ingest.socket')


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    server = Server()
    server.run()
    _LOGGER.debug("Shutting down controller")
    control.shutdown()


if __name__ == '__main__':
    main()
