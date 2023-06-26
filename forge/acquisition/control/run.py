import typing
import asyncio
import signal
import argparse
import logging
import importlib.util
from importlib import import_module
from forge.acquisition import CONFIGURATION
from .base import BaseControl


_LOGGER = logging.getLogger(__name__)


def arguments() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Forge acquisition control.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--systemd',
                        dest='systemd', action='store_true',
                        help="enable systemd integration")

    parser.add_argument('type',
                        help="control type code")

    return parser


def main():
    args, _ = arguments().parse_known_args()
    if args.debug:
        root_logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)-40s %(message)s')
        handler.setFormatter(formatter)
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)

    if '/' in args.type:
        spec = importlib.util.spec_from_file_location("forge.acquisition.control.external", args.type)
        importlib.util.module_from_spec(spec).main()
    else:
        import_module('.', 'forge.acquisition.control.' + args.type).main()


def run(control: BaseControl, systemd: bool = False) -> None:
    loop = asyncio.get_event_loop()

    async def start_bus():
        reader, writer = await asyncio.open_unix_connection(CONFIGURATION.get("ACQUISITION.BUS", '/run/forge-acquisition-bus.socket'))
        control.bus = control.BusClient(control, reader, writer)
        await control.bus.start()

    _LOGGER.debug("Starting bus interface")
    loop.run_until_complete(start_bus())

    _LOGGER.debug("Initialize control")
    loop.run_until_complete(control.initialize())

    heartbeat: typing.Optional[asyncio.Task] = None
    if systemd:
        import systemd.daemon
        systemd.daemon.notify("READY=1")

        _LOGGER.debug("Starting systemd heartbeat")

        async def send_heartbeat() -> typing.NoReturn:
            while True:
                await asyncio.sleep(10)
                systemd.daemon.notify("WATCHDOG=1")

        heartbeat = loop.create_task(send_heartbeat())

    _LOGGER.debug("Control start")
    control_run = loop.create_task(control.run())
    loop.add_signal_handler(signal.SIGINT, control_run.cancel)
    loop.add_signal_handler(signal.SIGTERM, control_run.cancel)
    try:
        loop.run_until_complete(control_run)
    except asyncio.CancelledError:
        pass
    _LOGGER.debug("Control shutdown")

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

    _LOGGER.debug("Control finish")
    loop.run_until_complete(control.finish())

    _LOGGER.debug("Shutting down bus interface")
    loop.run_until_complete(control.bus.shutdown())
    control.bus = None

    _LOGGER.debug("Shutdown complete")


def launch(control: typing.Type[BaseControl]) -> None:
    args = arguments()
    args = args.parse_args()
    run(control(), args.systemd)

