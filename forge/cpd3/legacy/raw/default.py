#!/usr/bin/env python3

import typing
import asyncio
import os
import logging
import argparse
import time
from math import floor, ceil
from tempfile import NamedTemporaryFile
from netCDF4 import Dataset
from forge.const import STATIONS as VALID_STATIONS
from forge.formattime import format_iso8601_time
from forge.timeparse import parse_time_argument
from forge.archive.client.connection import Connection
from forge.cpd3.convert.station.lookup import station_data
from forge.cpd3.legacy.raw.analyze import Instrument as AnalyzeInstrument
from forge.cpd3.legacy.raw.write import write_day
from forge.cpd3.legacy.instrument.default import convert_raw

_LOGGER = logging.getLogger(__name__)

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS
parser = argparse.ArgumentParser(description=f"CPD3 legacy conversion for {STATION.upper()} raw data")
parser.add_argument('--debug',
                    dest='debug', action='store_true',
                    help="enable debug output")
parser.add_argument('--start',
                    dest='start',
                    help="override start time")
parser.add_argument('--end',
                    dest='end',
                    help="override end time")
args = parser.parse_args()
if args.debug:
    from forge.log import set_debug_logger
    set_debug_logger()
DATA_START_TIME = parse_time_argument(args.start).timestamp() if args.start else station_data(STATION, 'legacy', 'DATA_START_TIME')
DATA_END_TIME = parse_time_argument(args.end).timestamp() if args.end else station_data(STATION, 'legacy', 'DATA_END_TIME')
assert DATA_START_TIME < DATA_END_TIME
begin_time = time.monotonic()
_LOGGER.info(f"Starting raw data conversion for {STATION.upper()} in {format_iso8601_time(DATA_START_TIME)} to {format_iso8601_time(DATA_END_TIME)}")

legacy_instruments = AnalyzeInstrument.scan_station(STATION, DATA_START_TIME, DATA_END_TIME)
total_files = 0

async def run():
    global total_files
    for instrument_id, segments in legacy_instruments.items():
        for legacy_instrument in segments:
            assert legacy_instrument.start < legacy_instrument.end
            start_day = int(floor(max(DATA_START_TIME, legacy_instrument.start) / (24 * 60 * 60))) * 24 * 60 * 60
            end_day = int(ceil(min(DATA_END_TIME, legacy_instrument.end) / (24 * 60 * 60))) * 24 * 60 * 60
            if end_day <= min(DATA_END_TIME, legacy_instrument.end):
                end_day += 24 * 60 * 60
            if start_day >= end_day:
                continue

            any_converted: bool = False
            for start_of_day in range(start_day, end_day, 24 * 60 * 60):
                end_of_day = start_of_day + 24 * 60 * 60
                if end_of_day > DATA_END_TIME:
                    incomplete_day = True
                    end_of_day = DATA_END_TIME
                else:
                    incomplete_day = False

                _LOGGER.debug(f"Converting {instrument_id} day {format_iso8601_time(start_of_day)}")
                with NamedTemporaryFile(suffix=".nc") as output_file:
                    root = Dataset(output_file.name, 'w', format='NETCDF4')
                    try:
                        if not convert_raw(legacy_instrument, STATION, instrument_id,
                                           start_of_day, end_of_day, root):
                            continue
                    finally:
                        root.close()

                    async with (await Connection.default_connection("write legacy raw")) as connection:
                        await write_day(connection, output_file.name, STATION, start_of_day, end_of_day, incomplete_day)
                    total_files += 1
                    any_converted = True
            if not any_converted:
                _LOGGER.warning(f"Unable to convert {instrument_id}: {str(legacy_instrument)}")



loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(run())
loop.close()

end_time = time.monotonic()
_LOGGER.info(f"Conversion of {len(legacy_instruments)} instruments in {total_files} files completed in {(end_time - begin_time):.2f} seconds")
