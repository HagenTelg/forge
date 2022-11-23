import typing
import asyncio
import signal
import argparse
import logging
from importlib import import_module
from pathlib import Path
from forge.acquisition import LayeredConfiguration, CONFIGURATION
from forge.acquisition.average import AverageRecord
from .base import BaseInstrument, BaseDataOutput, BaseBusInterface, BasePersistentInterface


_LOGGER = logging.getLogger(__name__)


def arguments() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Forge acquisition instrument.")

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

    parser.add_argument('type',
                        help="instrument type code")
    parser.add_argument('identifier',
                        help="instrument identifier code")

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

    import_module('.', 'forge.acquisition.instrument.' + args.type).main()


def instrument_config(args: argparse.Namespace) -> LayeredConfiguration:
    roots: typing.List[dict] = list()

    instrument_local = CONFIGURATION.get("INSTRUMENT." + args.identifier)
    toml = None
    if instrument_local is not None:
        roots.append(instrument_local)
        toml = LayeredConfiguration.configuration_toml("INSTRUMENT." + args.identifier)

    global_config = CONFIGURATION.get("ACQUISITION.GLOBAL")
    if global_config is not None:
        roots.append(global_config)
        if toml is None:
            toml = LayeredConfiguration.configuration_toml("ACQUISITION.GLOBAL")

    return LayeredConfiguration(*roots, toml=toml)


def average_config(args: argparse.Namespace) -> LayeredConfiguration:
    roots: typing.List[dict] = list()

    instrument_local = CONFIGURATION.get("INSTRUMENT." + args.identifier + ".AVERAGE")
    toml = None
    if instrument_local is not None:
        roots.append(instrument_local)
        toml = LayeredConfiguration.configuration_toml("INSTRUMENT." + args.identifier + ".AVERAGE")

    global_config = CONFIGURATION.get("ACQUISITION.AVERAGE")
    if global_config is not None:
        roots.append(global_config)
        if toml is None:
            toml = LayeredConfiguration.configuration_toml("ACQUISITION.AVERAGE")

    return LayeredConfiguration(*roots, toml=toml)


def cutsize_config(args: argparse.Namespace) -> LayeredConfiguration:
    roots: typing.List[dict] = list()

    instrument_local = CONFIGURATION.get("INSTRUMENT." + args.identifier + ".CUT_SIZE")
    toml = None
    if instrument_local is not None:
        roots.append(instrument_local)
        toml = LayeredConfiguration.configuration_toml("INSTRUMENT." + args.identifier + ".CUT_SIZE")

    global_config = CONFIGURATION.get("ACQUISITION.CUT_SIZE")
    if global_config is not None:
        roots.append(global_config)
        if toml is None:
            toml = LayeredConfiguration.configuration_toml("ACQUISITION.CUT_SIZE")

    return LayeredConfiguration(*roots, toml=toml)


def bus_interface(args: argparse.Namespace) -> BaseBusInterface:
    from .businterface import BusInterface

    return BusInterface(args.identifier, CONFIGURATION.get("ACQUISITION.BUS", '/run/forge-acquisition-bus.socket'))


def data_directories(args: argparse.Namespace) -> typing.Tuple[typing.Optional[Path], typing.Optional[Path]]:
    working_directory = args.data_working or CONFIGURATION.get("ACQUISITION.DATA_TEMP")
    if working_directory:
        working_directory = Path(working_directory)
    else:
        working_directory = None

    completed_directory = args.data_completed or CONFIGURATION.get("ACQUISITION.DATA_COMPLETED")
    if completed_directory:
        completed_directory = Path(completed_directory)
    else:
        completed_directory = None

    return working_directory, completed_directory


def data_output(args: argparse.Namespace) -> BaseDataOutput:
    from .dataoutput import DataOutput

    roots: typing.List[dict] = list()

    instrument_local = CONFIGURATION.get("INSTRUMENT." + args.identifier + ".FILE")
    toml = None
    if instrument_local is not None:
        roots.append(instrument_local)
        toml = LayeredConfiguration.configuration_toml("INSTRUMENT." + args.identifier + ".FILE")

    global_config = CONFIGURATION.get("ACQUISITION.FILE")
    if global_config is not None:
        roots.append(global_config)
        if toml is None:
            toml = LayeredConfiguration.configuration_toml("ACQUISITION.FILE")

    working_directory, completed_directory = data_directories(args)

    return DataOutput(CONFIGURATION.get("ACQUISITION.STATION", 'nil').lower(), args.identifier,
                      LayeredConfiguration(*roots, toml=toml),
                      working_directory, completed_directory,
                      AverageRecord(average_config(args)).interval)


def persistent_interface(args: argparse.Namespace) -> BasePersistentInterface:
    from .persistent import PersistentInterface

    state_file = f"{args.identifier}-{args.type.lower()}.json"
    state_location = Path(CONFIGURATION.get("ACQUISITION.STATE_LOCATION", "/var/lib/forge/state"))

    return PersistentInterface(state_location / state_file)


def prepare_context(instrument: BaseInstrument) -> None:
    from .dataoutput import DataOutput
    data: DataOutput = instrument.context.data

    data.instrument_type = instrument.INSTRUMENT_TYPE
    data.tags.update(instrument.TAGS)

    def add_tags(tags):
        if isinstance(tags, str):
            tags = tags.split()
        if isinstance(tags, bool):
            return
        for t in tags:
            t = t.strip()
            if not t:
                continue
            data.tags.add(t)

    tags = instrument.context.config.get("TAGS")
    if tags:
        add_tags(tags)
    tags = instrument.context.config.get("ALL_TAGS")
    if tags:
        data.tags.clear()
        add_tags(tags)


def run(instrument: BaseInstrument, systemd: bool = False) -> None:
    loop = asyncio.get_event_loop()
    prepare_context(instrument)

    _LOGGER.debug("Starting bus interface")
    loop.run_until_complete(instrument.context.bus.start())

    _LOGGER.debug("Starting data interface")
    loop.run_until_complete(instrument.context.data.start())

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

    _LOGGER.debug("Instrument start")
    instrument_run = loop.create_task(instrument.run())
    loop.add_signal_handler(signal.SIGINT, instrument_run.cancel)
    loop.add_signal_handler(signal.SIGTERM, instrument_run.cancel)
    try:
        loop.run_until_complete(instrument_run)
    except asyncio.CancelledError:
        pass
    _LOGGER.debug("Instrument shutdown")

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

    _LOGGER.debug("Shutting down data interface")
    loop.run_until_complete(instrument.context.data.shutdown())

    _LOGGER.debug("Shutting down bus interface")
    loop.run_until_complete(instrument.context.bus.shutdown())

    _LOGGER.debug("Shutdown complete")

