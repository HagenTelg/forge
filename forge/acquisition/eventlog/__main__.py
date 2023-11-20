import typing
import asyncio
import logging
import argparse
import signal
from pathlib import Path
from forge.acquisition import CONFIGURATION, LayeredConfiguration
from forge.acquisition.bus.client import AcquisitionBusClient
from forge.acquisition.instrument.run import data_directories
from .log import Log

_LOGGER = logging.getLogger(__name__)


class Client(AcquisitionBusClient):
    def __init__(self, log: Log, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__("_LOG", reader, writer)
        self.log = log

    async def incoming_message(self, source: str, record: str, message: typing.Any) -> None:
        if record != 'event_log':
            return
        self.log.add_message(source, message)


def main():
    parser = argparse.ArgumentParser(description="Forge acquisition event log controller.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--systemd',
                        dest='systemd', action='store_true',
                        help="enable systemd integration")

    parser.add_argument('--data-working',
                        dest='data_working',
                        help="directory to place in progress data files")
    parser.add_argument('--data-completed',
                        dest='data_completed',
                        help="directory to place in completed data files")

    args = parser.parse_args()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    bus_socket = CONFIGURATION.get("ACQUISITION.BUS", '/run/forge-acquisition-bus.socket')

    roots: typing.List[dict] = list()
    log_local = CONFIGURATION.get("ACQUISITION.EVENT_LOG")
    if log_local:
        roots.append(log_local)
    global_config = CONFIGURATION.get("ACQUISITION.GLOBAL")
    if global_config:
        roots.append(global_config)
    config = LayeredConfiguration(*roots)

    station = CONFIGURATION.get("ACQUISITION.STATION", 'nil').lower()
    working_directory, completed_directory = data_directories(args)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    log = Log(station, config, working_directory, completed_directory)
    client: typing.Optional[Client] = None

    async def start():
        nonlocal client

        _LOGGER.debug("Starting bus interface")
        reader, writer = await asyncio.open_unix_connection(bus_socket)
        client = Client(log, reader, writer)
        await client.start()

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

    _LOGGER.debug("Event log start")
    log_run = loop.create_task(log.run())
    loop.add_signal_handler(signal.SIGINT, log_run.cancel)
    loop.add_signal_handler(signal.SIGTERM, log_run.cancel)
    try:
        loop.run_until_complete(log_run)
    except asyncio.CancelledError:
        pass
    _LOGGER.debug("Event log shutdown")

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

    _LOGGER.debug("Shutting down bus interface")
    loop.run_until_complete(client.shutdown())

    _LOGGER.debug("Shutting down event log output")
    loop.run_until_complete(log.shutdown())

    loop.close()
    _LOGGER.debug("Shutdown complete")


if __name__ == '__main__':
    main()
