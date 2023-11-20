import typing
import asyncio
import logging
import time
import sys
from netCDF4 import Dataset
from pathlib import Path
from forge.archive.client.connection import Connection, LockDenied, LockBackoff
from forge.archive.client.put import ArchivePut, InvalidFile
from forge.processing.station.lookup import station_data
from forge.dashboard.report.action import DashboardAction
from forge.dashboard.report.send import dashboard_action, report_ok

_LOGGER = logging.getLogger(__name__)


async def file_corrupted(file_name: Path, station: str, elapsed: float) -> None:
    # Processing ok, file failed
    await report_ok('acquisition-ingest-data', station, events=[{
        "code": "file-processed",
        "severity": "warning",
        "data": f"{file_name.name},{file_name.stat().st_size},{int(elapsed * 1000.0)}",
        "occurred_at": time.time()
    }], unreported_exception=False)


async def file_error(file_name: Path, station: str, elapsed: float) -> None:
    # Processing ok, file failed
    try:
        # Errors often mean the file is not accessible
        st = file_name.stat()
    except OSError:
        st = None
    await report_ok('acquisition-ingest-data', station, events=[{
        "code": "file-processed",
        "severity": "error",
        "data": f"{file_name.name},{st.st_size if st else 0},{int(elapsed * 1000.0)}",
        "occurred_at": time.time()
    }], unreported_exception=False)


async def file_complete(file_name: Path, file_data: Dataset, station: str, elapsed: float) -> None:
    begin_analysis = time.monotonic()

    action = DashboardAction(station, 'acquisition-ingest-data')
    action.failed = False

    try:
        file_station = ArchivePut.get_station(file_data)
        if station != file_station:
            action.notifications.add(action.Notification(
                'station-mismatch', action.Severity.WARNING,
                f"Initial station was {station.upper()} while the file reports {file_station.upper()}"
            ))
    except InvalidFile:
        pass

    station_data(station, 'dashboard', 'analyze_acquisition')(station, file_data, action)

    elapsed += time.monotonic() - begin_analysis
    action.events.append(action.Event(
        "file-processed", action.Severity.INFO,
        f"{file_name.name},{file_name.stat().st_size},{int(elapsed * 1000.0)}"
    ))

    await dashboard_action(action, unreported_exception=False)


async def process_file(connection: Connection, file_name: Path, station: str = None) -> None:
    begin_processing = time.monotonic()
    try:
        file_data = Dataset(str(file_name), 'r')
    except FileNotFoundError:
        raise
    except OSError:
        if station:
            await file_corrupted(file_name, station, time.monotonic() - begin_processing)
        raise

    backoff = LockBackoff()
    try:
        while True:
            try:
                async with connection.transaction(True):
                    put = ArchivePut(connection)
                    await put.auto(file_data, station=station)
                    await put.commit_index()
                break
            except LockDenied as ld:
                _LOGGER.debug("Archive busy: %s", ld.status)
                if sys.stdout.isatty():
                    if not backoff.has_failed:
                        sys.stdout.write("\n")
                    sys.stdout.write(f"\x1B[2K\rBusy: {ld.status}")
                await backoff()

        if not station:
            try:
                station = ArchivePut.get_station(file_data)
            except InvalidFile:
                pass

        if station:
            await file_complete(file_name, file_data, station, time.monotonic() - begin_processing)
    except InvalidFile:
        if not station:
            try:
                station = ArchivePut.get_station(file_data)
            except InvalidFile:
                pass
        if station:
            await file_corrupted(file_name, station, time.monotonic() - begin_processing)
        raise
    finally:
        if backoff.has_failed and sys.stdout.isatty():
            sys.stdout.write("\n")
        file_data.close()


def main():
    import argparse
    from forge.archive import CONFIGURATION

    parser = argparse.ArgumentParser(description="Forge acquisition data file ingest.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--server-host',
                        dest='tcp_server',
                        help="archive server host")
    group.add_argument('--server-socket',
                        dest='unix_socket',
                        help="archive server Unix socket")
    parser.add_argument('--server-port',
                        dest='tcp_port',
                        type=int,
                        default=CONFIGURATION.get("ARCHIVE.PORT"),
                        help="archive server port")

    parser.add_argument('--station',
                        dest='station',
                        help="override destination station")

    parser.add_argument('file',
                        help="files to process",
                        nargs='+')

    args = parser.parse_args()
    if args.tcp_server and not args.tcp_port:
        parser.error("Both a server host and port must be specified")

    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    for file_name in args.file:
        check = Path(file_name)
        if not check.exists() or not check.is_file():
            _LOGGER.error(f"File {file_name} does not exist")
            exit(1)

    loop = asyncio.new_event_loop()

    async def run():
        if args.tcp_server and args.tcp_port:
            _LOGGER.debug(f"Connecting to archive TCP socket {args.tcp_server}:{args.tcp_port}")
            reader, writer = await asyncio.open_connection(args.tcp_server, int(args.tcp_port))
            connection = Connection(reader, writer, "data ingest")
        elif args.unix_socket:
            _LOGGER.debug(f"Connecting to archive Unix socket {args.unix_socket}")
            reader, writer = await asyncio.open_unix_connection(args.unix_socket)
            connection = Connection(reader, writer, "data ingest")
        else:
            connection = await Connection.default_connection("data ingest")

        await connection.startup()

        for file_name in args.file:
            _LOGGER.debug("Ingesting %s", file_name)
            await process_file(connection, Path(file_name), station=args.station)

        await connection.shutdown()

    loop.run_until_complete(run())
    loop.close()


if __name__ == '__main__':
    main()
