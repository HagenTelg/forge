import typing
import asyncio
import logging
import argparse
from math import floor, ceil
from pathlib import Path
from forge.const import STATIONS
from forge.timeparse import parse_time_bounds_arguments

_LOGGER = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Forge NCEI file generation.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--directory',
                        dest='directory',
                        help="output directory instead of the current one")
    parser.add_argument('--data',
                        dest='data', default='aerosol',
                        help="NCEI file type code")
    parser.add_argument('station',
                        help="station code")
    parser.add_argument('time', help="time bounds to generate", nargs='+')

    args = parser.parse_args()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    station = args.station.lower()
    if station not in STATIONS:
        parser.error("Invalid station code")
    start, end = parse_time_bounds_arguments(args.time)
    start = start.timestamp()
    end = end.timestamp()
    start_epoch_ms = int(floor(start * 1000))
    end_epoch_ms = int(ceil(end * 1000))

    file_type_code = args.data.lower()

    _LOGGER.debug(f"Looking up {file_type_code} for {station} in {start_epoch_ms},{end_epoch_ms}")
    from forge.processing.station.lookup import station_data
    try:
        converter = station_data(station, 'ncei', 'file')(
            station, file_type_code, start_epoch_ms, end_epoch_ms
        )
    except FileNotFoundError:
        parser.error(f"NCEI file type code '{file_type_code}' no found for station and/or time")
        exit(1)
    converter = converter(station, start_epoch_ms, end_epoch_ms)

    async def run():
        output_directory = Path(args.directory) if args.directory else Path(".")
        _LOGGER.debug(f"Writing file for {file_type_code} to {output_directory}")
        await converter(output_directory)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.close()


if __name__ == '__main__':
    main()
