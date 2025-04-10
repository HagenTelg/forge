#!/usr/bin/env python3

import typing
import asyncio
import os
import logging
import argparse
import time
from math import floor, ceil
from concurrent.futures import ThreadPoolExecutor
from forge.const import STATIONS as VALID_STATIONS
from forge.formattime import format_iso8601_time
from forge.timeparse import parse_time_argument
from forge.archive.client.connection import Connection
from forge.cpd3.convert.station.lookup import station_data
from forge.cpd3.legacy.readarchive import read_archive, Selection
from forge.cpd3.legacy.eventlog.write import write_day, Event

_LOGGER = logging.getLogger(__name__)

STATION = "bou"
parser = argparse.ArgumentParser(description=f"CPD3 legacy conversion for {STATION.upper()} event log")
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
_LOGGER.info(f"Starting event log conversion for {STATION.upper()} in {format_iso8601_time(DATA_START_TIME)} to {format_iso8601_time(DATA_END_TIME)}")

total_events: int = 0
netcdf_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="NetCDFWorker")

async def run():
    global total_events
    start_day = int(floor(DATA_START_TIME / (24 * 60 * 60))) * 24 * 60 * 60
    end_day = int(ceil(DATA_END_TIME / (24 * 60 * 60))) * 24 * 60 * 60
    for start_of_day in range(start_day, end_day, 24 * 60 * 60):
        end_of_day = start_of_day + 24 * 60 * 60
        if end_of_day > DATA_END_TIME:
            incomplete_day = True
            end_of_day = DATA_END_TIME
        else:
            incomplete_day = False

        _LOGGER.debug(f"Converting day {format_iso8601_time(start_of_day)}")

        converted_events: typing.List[Event] = list()
        for identity, info, modified in read_archive([Selection(
                start=start_of_day,
                end=end_of_day,
                stations=["bao"],
                archives=["events"],
                variables=["acquisition"],
                include_meta_archive=False,
                include_default_station=False,
        )]):
            if not identity.start or identity.start < start_of_day or identity.end > end_of_day:
                continue
            converted_events.append(Event(identity, info, modified))

        if not converted_events:
            _LOGGER.debug("No events in day")
            continue

        _LOGGER.debug(f"Writing {len(converted_events)} events")
        async with (await Connection.default_connection("write legacy eventlog")) as connection:
            await write_day(connection, converted_events, STATION, start_of_day, end_of_day, incomplete_day,
                            netcdf_executor=netcdf_executor)
        total_events += len(converted_events)


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(run())
loop.close()

end_time = time.monotonic()
_LOGGER.info(f"Conversion of {total_events} events completed in {(end_time - begin_time):.2f} seconds")
