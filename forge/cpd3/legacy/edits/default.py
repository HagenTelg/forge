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
from write import write_all, EditDirective

_LOGGER = logging.getLogger(__name__)


def main(station: str, edit_directive: typing.Type[EditDirective]):
    parser = argparse.ArgumentParser(description=f"CPD3 legacy conversion for {station.upper()} edit directives")
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
    DATA_START_TIME = parse_time_argument(args.start).timestamp() if args.start else station_data(station, 'legacy',
                                                                                                  'DATA_START_TIME')
    DATA_END_TIME = parse_time_argument(args.end).timestamp() if args.end else station_data(station, 'legacy',
                                                                                            'DATA_END_TIME')
    assert DATA_START_TIME < DATA_END_TIME
    begin_time = time.monotonic()
    _LOGGER.info(
        f"Starting edit directive conversion for {station.upper()} in {format_iso8601_time(DATA_START_TIME)} to {format_iso8601_time(DATA_END_TIME)}")

    year_data: typing.Dict[int, typing.List[EditDirective]] = dict()
    cpd3_edits = read_archive([Selection(
        start=DATA_START_TIME,
        end=DATA_END_TIME,
        stations=[station],
        archives=["edits"],
        include_meta_archive=False,
        include_default_station=False,
    )])
    _LOGGER.debug(f"Loaded {len(cpd3_edits)} CPD3 edit directives")
    allocated_uids: typing.Set[int] = set()
    for identity, info, modified in cpd3_edits:
        if identity.start and identity.end and identity.start >= identity.end:
            _LOGGER.debug(f"Skipping zero length edit at {format_iso8601_time(identity.start)}")
            continue
        converted = edit_directive(identity, info, modified, allocated_uids=allocated_uids)
        if converted.skip_conversion:
            continue
        if converted.is_clap_correction:
            _LOGGER.info(f"Ignoring CLAP correction edit {converted}")
            continue

        for year in range(*converted.affected_years):
            dest = year_data.get(year)
            if not dest:
                dest = list()
                year_data[year] = dest
            dest.append(converted)

    total, modified = write_all(station, year_data, DATA_START_TIME, DATA_END_TIME)

    end_time = time.monotonic()
    _LOGGER.info(
        f"Conversion of {len(cpd3_edits)} ({modified}/{total}) edit directives completed in {(end_time - begin_time):.2f} seconds")


if __name__ == '__main__':
    station = os.path.basename(__file__).split('.', 1)[0].lower()
    assert station in VALID_STATIONS
    main(station, EditDirective)

