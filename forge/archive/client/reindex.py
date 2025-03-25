import typing
import asyncio
import logging
import time
from tempfile import NamedTemporaryFile
from netCDF4 import Dataset
from pathlib import Path
from forge.const import STATIONS
from forge.logicaltime import year_bounds_ms
from forge.archive.client import data_lock_key, data_file_name, index_lock_key, index_file_name, index_instrument_history_file_name
from forge.archive.client.connection import Connection
from forge.archive.client.archiveindex import ArchiveIndex
from forge.archive.client.instrumenthistory import InstrumentHistory

_LOGGER = logging.getLogger(__name__)


async def reindex(connection: Connection, station: str, archive: str, year: int) -> None:
    year_start, year_end = year_bounds_ms(year)
    await connection.lock_write(data_lock_key(station, archive), year_start, year_end)
    await connection.lock_write(index_lock_key(station, archive), year_start, year_end)
    file_prefix = station.upper() + "-"
    ts = time.gmtime(int(year_start / 1000.0))
    available_instruments: typing.Set[str] = set()
    for file in await connection.list_files(f"data/{station.lower()}/{archive.lower()}/{ts.tm_year:04}/"):
        file = Path(file).name
        if file == '_index.json' or file == '_history.json':
            continue
        if not file.startswith(file_prefix):
            continue
        instrument = file[len(file_prefix):].split('_', 1)[0]
        if not instrument:
            continue
        available_instruments.add(instrument)

    if not available_instruments:
        _LOGGER.debug("No available instruments for %s/%s/%d, removing index", station, archive, year)
        try:
            await connection.remove_file(index_file_name(station, archive, year_start))
        except FileNotFoundError:
            pass
        return

    _LOGGER.debug("Found %d instruments for %s/%s/%d", len(available_instruments), station, archive, year)

    index = ArchiveIndex()
    if archive == "raw":
        history = InstrumentHistory()
    else:
        history = None

    async def integrate_file(name: str) -> bool:
        with NamedTemporaryFile(suffix=".nc") as data_file:
            try:
                await connection.read_file(name, data_file)
                data_file.flush()
                existing_data = Dataset(data_file.name, 'r')
                index.integrate_file(existing_data)
                if history:
                    history.update_file(existing_data)
                existing_data.close()
                return True
            except FileNotFoundError:
                return False

    any_valid = False
    for instrument_id in available_instruments:
        count = 0
        if archive == "avgm":
            if await integrate_file(data_file_name(station, archive, instrument_id, year_start / 1000.0)):
                count += 1
        else:
            for file_start in range(year_start, year_end, 24 * 60 * 60 * 1000):
                if await integrate_file(data_file_name(station, archive, instrument_id, file_start / 1000.0)):
                    count += 1

        _LOGGER.debug("Indexed %d files for %s", count, instrument_id)
        if count:
            any_valid = True

    if not any_valid:
        _LOGGER.debug("No valid data found, removing index")
        try:
            await connection.remove_file(index_file_name(station, archive, year_start / 1000.0))
            if archive == "raw":
                await connection.remove_file(index_instrument_history_file_name(station, archive, year_start / 1000.0))
        except FileNotFoundError:
            pass
        return

    index_contents = index.commit()
    _LOGGER.debug("Final index size %d bytes", len(index_contents))
    await connection.write_bytes(index_file_name(station, archive, year_start / 1000.0), index_contents)
    if history:
        history_contents = history.commit()
        _LOGGER.debug("Final history size %d bytes", len(history_contents))
        await connection.write_bytes(index_instrument_history_file_name(station, archive, year_start / 1000.0), history_contents)


def main():
    import argparse
    import sys
    from forge.archive import CONFIGURATION
    from forge.archive.client.connection import Connection, LockDenied, LockBackoff

    parser = argparse.ArgumentParser(description="Forge archive reindex.")

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
                        help="limit reindexing to a station")
    parser.add_argument('--archive',
                        dest='archive',
                        choices=["raw", "edited", "clean", "avgh", "avgd", "avgm"],
                        help="limit reindexing to an archive")
    parser.add_argument('--year',
                        dest='year', type=int,
                        help="limit reindexing to a year")

    args = parser.parse_args()
    if args.tcp_server and not args.tcp_port:
        parser.error("Both a server host and port must be specified")

    if args.station:
        station = args.station.lower()
        if args.station not in STATIONS:
            parser.error("Invalid station")
        reindex_stations = [station]
    else:
        reindex_stations = sorted(STATIONS)

    if args.archive:
        reindex_archives = [args.archive.lower()]
    else:
        reindex_archives = ["raw", "edited", "clean", "avgh", "avgd", "avgm"]

    if args.year:
        year = int(args.year)
        if year <= 0:
            ts = time.gmtime()
            year = ts.tm_year + year
        if year < 1971 or year > 2999:
            parser.error("Invalid year")
        reindex_years = range(year, year+1)
    else:
        ts = time.gmtime()
        reindex_years = range(1971, ts.tm_year+1)

    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    loop = asyncio.new_event_loop()

    async def run():
        if args.tcp_server and args.tcp_port:
            _LOGGER.debug(f"Connecting to archive TCP socket {args.tcp_server}:{args.tcp_port}")
            reader, writer = await asyncio.open_connection(args.tcp_server, int(args.tcp_port))
            connection = Connection(reader, writer, "reindex")
        elif args.unix_socket:
            _LOGGER.debug(f"Connecting to archive Unix socket {args.unix_socket}")
            reader, writer = await asyncio.open_unix_connection(args.unix_socket)
            connection = Connection(reader, writer, "reindex")
        else:
            connection = await Connection.default_connection("reindex")

        await connection.startup()

        for station in reindex_stations:
            for archive in reindex_archives:
                for year in reindex_years:
                    _LOGGER.debug("Starting reindex for %s/%s/%d", station, archive, year)
                    backoff = LockBackoff()
                    try:
                        while True:
                            try:
                                async with connection.transaction(True):
                                    await reindex(connection, station, archive, year)
                                break
                            except LockDenied as ld:
                                _LOGGER.debug("Archive busy: %s", ld.status)
                                if sys.stdout.isatty():
                                    if not backoff.has_failed:
                                        sys.stdout.write("\n")
                                    sys.stdout.write(f"\x1B[2K\rBusy: {ld.status}")
                                await backoff()
                    finally:
                        if backoff.has_failed and sys.stdout.isatty():
                            sys.stdout.write("\n")

        await connection.shutdown()

    loop.run_until_complete(run())
    loop.close()


if __name__ == '__main__':
    main()
