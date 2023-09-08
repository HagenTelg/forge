import typing
import asyncio
import logging
import struct
from forge.vis import CONFIGURATION
from forge.service import SocketServer
from .protocol import ConnectionType
from .connection import Station


_LOGGER = logging.getLogger(__name__)


_stations: typing.Dict[str, Station] = dict()


class Server(SocketServer):
    DESCRIPTION = "Forge visualization acquisition server."

    async def connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        async def string_arg() -> str:
            arg_len = struct.unpack('<I', await reader.readexactly(4))[0]
            return (await reader.readexactly(arg_len)).decode('utf-8')

        station: typing.Optional[str] = None
        station_instance: typing.Optional[Station] = None
        connection: typing.Optional[Station.Connection] = None
        try:
            connection_type = ConnectionType(struct.unpack('<B', await reader.readexactly(1))[0])

            if connection_type == ConnectionType.ACQUISITION:
                station = await string_arg()
                station = station.lower()

                station_instance = _stations.get(station)
                if not station_instance:
                    _LOGGER.debug(f"Creating station {station}")
                    station_instance = Station(station)
                    _stations[station] = station_instance

                writer.write(struct.pack('<B', 1))
                await writer.drain()

                _LOGGER.debug(f"Accepted acquisition connection for station {station}")
                connection = station_instance.attach_acquisition(reader, writer)
                await connection.run()
            elif connection_type == ConnectionType.DISPLAY:
                station = await string_arg()
                station = station.lower()

                station_instance = _stations.get(station)
                if station_instance is None:
                    _LOGGER.debug(f"No acquisition connection available for {station}")
                    return

                writer.write(struct.pack('<B', 1))
                await writer.drain()

                _LOGGER.debug(f"Accepted acquisition connection for station {station}")
                connection = station_instance.attach_display(reader, writer)
                await connection.run()
            else:
                raise ValueError("Invalid connection type")
        except:
            _LOGGER.debug("Error in connection", exc_info=True)
        finally:
            if station_instance and connection:
                if station_instance.detach(connection):
                    del _stations[station]
                    _LOGGER.debug(f"Removed station {station}")

            try:
                writer.close()
            except OSError:
                pass
            return

    @property
    def default_socket(self) -> str:
        return CONFIGURATION.get('ACQUISITION.SOCKET', '/run/forge-vis-acquisition.socket')


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    server = Server()
    server.run()


if __name__ == '__main__':
    main()
