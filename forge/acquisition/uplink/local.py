import typing
import asyncio
import logging
import argparse
import signal
from forge.vis.realtime.translation import get_translator as get_realtime_translator, RealtimeTranslator
from forge.vis.realtime.controller.client import WriteData as RealtimeOutput
from forge.vis.acquisition.translation import get_translator as get_acquisition_translator, AcquisitionTranslator
from forge.acquisition.bus.client import AcquisitionBusClient
from . import CONFIGURATION
from .bus import BusInterface, PersistenceLevel
from .realtime import RealtimeTranslatorOutput
from .acquisition import AcquisitionTranslatorClient


_LOGGER = logging.getLogger(__name__)


class LocalBusClient(AcquisitionBusClient, BusInterface):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 realtime: typing.Optional[RealtimeTranslatorOutput],
                 acquisition: typing.Optional[AcquisitionTranslatorClient]):
        AcquisitionBusClient.__init__(self, "_UPLINK", reader, writer, disable_echo=True)
        self.realtime = realtime
        self.acquisition = acquisition

    async def incoming_message(self, source: str, record: str, message: typing.Any) -> None:
        if self.acquisition:
            self.acquisition.incoming_message(source, record, message)
        if self.realtime:
            self.realtime.incoming_message(source, record, message)

    async def send_message(self, level: PersistenceLevel, record: str, message: typing.Any) -> None:
        AcquisitionBusClient.send_message(self, level, record, message)


def main():
    parser = argparse.ArgumentParser(description="Acquisition direct bus uplink.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--systemd',
                        dest='systemd', action='store_true',
                        help="enable systemd service integration")

    parser.add_argument('--instantaneous',
                        dest='use_instantaneous', action='store_true',
                        help="use instantaneous data")
    parser.add_argument('--no-instantaneous',
                        dest='use_instantaneous', action='store_false',
                        help="do not use instantaneous data")
    parser.set_defaults(use_instantaneous=True)

    parser.add_argument('--generic-translator',
                        dest='generic_translator', action='store_true',
                        help="use generic translators if needed")

    parser.add_argument('--bus-socket',
                        dest='bus_socket', type=str,
                        default=CONFIGURATION.get('ACQUISITION.BUS', '/run/forge-acquisition-bus.socket'),
                        help="acquisition bus socket")
    parser.add_argument('--realtime-socket',
                        dest='realtime_socket', type=str,
                        default=CONFIGURATION.get('REALTIME.SOCKET', '/run/forge-vis-realtime.socket'),
                        help="realtime controller socket")
    parser.add_argument('--controller-socket',
                        dest='controller_socket', type=str,
                        default=CONFIGURATION.get('ACQUISITION.SOCKET', '/run/forge-vis-acquisition.socket'),
                        help="acquisition controller socket")

    parser.add_argument('--station',
                        dest='station', type=str,
                        default=CONFIGURATION.get("ACQUISITION.STATION", 'nil').lower(),
                        help="station code")

    args = parser.parse_args()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    station = args.station.lower()
    realtime_socket_name = args.realtime_socket
    acquisition_socket_name = args.controller_socket

    bus_client: typing.Optional[LocalBusClient] = None

    async def start():
        nonlocal bus_client

        _LOGGER.debug("Connecting to acquisition bus")
        bus_reader, bus_writer = await asyncio.open_unix_connection(args.bus_socket)

        realtime_output: typing.Optional[RealtimeTranslatorOutput] = None
        acquisition_client: typing.Optional[AcquisitionTranslatorClient] = None

        if realtime_socket_name:
            translator = get_realtime_translator(station)
            if translator and isinstance(translator, RealtimeTranslator):
                _LOGGER.debug(f"Connecting realtime translator for {station} to {realtime_socket_name}")
                try:
                    reader, writer = await asyncio.open_unix_connection(realtime_socket_name)
                    output = RealtimeOutput(reader, writer)
                    await output.connect()
                    realtime_output = RealtimeTranslatorOutput(station, output, translator)
                except OSError:
                    _LOGGER.warning(f"Failed to connect realtime data socket {realtime_socket_name}", exc_info=True)

        if acquisition_socket_name:
            translator = get_acquisition_translator(station)
            if translator and isinstance(translator, AcquisitionTranslator):
                _LOGGER.debug(f"Connecting acquisition translator for {station} to {acquisition_socket_name}")
                try:
                    reader, writer = await asyncio.open_unix_connection(acquisition_socket_name)
                    acquisition_client = AcquisitionTranslatorClient(reader, writer, translator,
                                                                     args.use_instantaneous)
                    await acquisition_client.connect(station, False)
                except OSError:
                    _LOGGER.warning(f"Failed to connect acquisition controller socket {acquisition_socket_name}",
                                    exc_info=True)

        bus_client = LocalBusClient(bus_reader, bus_writer, realtime_output, acquisition_client)
        if bus_client.acquisition:
            bus_client.acquisition.bus = bus_client

        _LOGGER.debug("Starting acquisition bus")
        await bus_client.start()

        if bus_client.realtime:
            _LOGGER.debug("Starting realtime data output")
            await bus_client.realtime.start()

        if bus_client.acquisition:
            _LOGGER.debug("Starting acquisition display output")
            await bus_client.acquisition.start()

    loop.run_until_complete(start())

    heartbeat: typing.Optional[asyncio.Task] = None
    if args.systemd:
        import systemd.daemon
        systemd.daemon.notify("READY=1")

        _LOGGER.debug("Starting systemd heartbeat")

        async def send_heartbeat() -> typing.NoReturn:
            while True:
                await asyncio.sleep(10)
                systemd.daemon.notify("WATCHDOG=1")

        heartbeat = loop.create_task(send_heartbeat())

    _LOGGER.debug("Local visualization uplink start")
    bus_wait = loop.create_task(bus_client.wait())
    loop.add_signal_handler(signal.SIGINT, bus_wait.cancel)
    loop.add_signal_handler(signal.SIGTERM, bus_wait.cancel)
    try:
        loop.run_until_complete(bus_wait)
    except asyncio.CancelledError:
        pass
    _LOGGER.debug("Local visualization uplink stop")

    if heartbeat:
        _LOGGER.debug("Shutting down heartbeat")
        t = heartbeat
        heartbeat = None
        try:
            t.cancel()
        except:
            pass
        try:
            loop.run_until_complete(t)
        except:
            pass

    if bus_client.realtime:
        _LOGGER.debug("Shutting down realtime output")
        loop.run_until_complete(bus_client.realtime.shutdown())
    bus_client.realtime = None

    if bus_client.acquisition:
        _LOGGER.debug("Shutting down acquisition display output")
        loop.run_until_complete(bus_client.acquisition.shutdown())
    bus_client.acquisition = None

    _LOGGER.debug("Shutting down bus interface")
    loop.run_until_complete(bus_client.shutdown())

    loop.close()
    _LOGGER.debug("Shutdown complete")


if __name__ == '__main__':
    main()
