#!/usr/bin/env python3

import typing
import os
import logging
import argparse
import time
from forge.const import STATIONS as VALID_STATIONS
from forge.formattime import format_iso8601_time
from forge.timeparse import parse_time_argument
from forge.cpd3.convert.station.lookup import station_data
from forge.cpd3.legacy.readarchive import read_archive, Selection
from write import write_all, PassedTime

_LOGGER = logging.getLogger(__name__)

STATION = "bou"
assert STATION in VALID_STATIONS
parser = argparse.ArgumentParser(description=f"CPD3 legacy conversion for {STATION.upper()} passed data")
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
_LOGGER.info(f"Starting passed flag conversion for {STATION.upper()} in {format_iso8601_time(DATA_START_TIME)} to {format_iso8601_time(DATA_END_TIME)}")

year_data: typing.Dict[int, typing.List[PassedTime]] = dict()
cpd3_passed = read_archive([Selection(
    start=DATA_START_TIME,
    end=DATA_END_TIME,
    stations=["bao"],
    archives=["passed"],
    include_meta_archive=False,
    include_default_station=False,
)])
_LOGGER.debug(f"Loaded {len(cpd3_passed)} CPD3 passes")
for identity, info, modified in cpd3_passed:
    if not identity.start:
        identity.start = DATA_START_TIME
    if not identity.end:
        identity.end = DATA_END_TIME

    converted = PassedTime(identity, info, modified)

    if converted.profile == "aod":
        # Moved out of data system handling
        continue

    for year in range(*converted.affected_years):
        dest = year_data.get(year)
        if not dest:
            dest = list()
            year_data[year] = dest
        dest.append(converted)

total, modified = write_all(STATION, year_data)

end_time = time.monotonic()
_LOGGER.info(f"Conversion of {len(cpd3_passed)} ({modified}/{total}) passes completed in {(end_time - begin_time):.2f} seconds")
