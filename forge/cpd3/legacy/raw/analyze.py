#!/usr/bin/env python3

import typing
import asyncio
import logging
import argparse
import time
import forge.cpd3.variant as variant
from math import floor, ceil, nan
from forge.const import STATIONS as VALID_STATIONS
from forge.formattime import format_iso8601_time
from forge.timeparse import parse_time_argument
from forge.cpd3.convert.station.lookup import station_data
from forge.cpd3.legacy.readarchive import read_archive, Selection

_LOGGER = logging.getLogger(__name__)


class Instrument:
    class Identifier:
        def __init__(self):
            self.cpd3_component: typing.Optional[str] = None
            self.forge_instrument: typing.Optional[str] = None

        def __eq__(self, other):
            if not isinstance(other, Instrument.Identifier):
                return False
            if self.cpd3_component == other.cpd3_component:
                return True
            return self.forge_instrument == other.forge_instrument

        def __ne__(self, other):
            return not (self == other)

        def __str__(self):
            if self.cpd3_component:
                return f"CPD3: {self.cpd3_component}"
            elif self.forge_instrument:
                return f"Forge: {self.forge_instrument}"
            else:
                return "UNKNOWN"

        def __bool__(self):
            return bool(self.cpd3_component) or bool(self.forge_instrument)

    def __init__(self):
        self.start: float = nan
        self.end: float = nan
        self.source = self.Identifier()
        self.variables: typing.Set[str] = set()

        self._last_identifier: typing.Dict[str, typing.Any] = dict()

    def __repr__(self) -> str:
        return f"Instrument({format_iso8601_time(self.start)}, {format_iso8601_time(self.end)}, {str(self.source)})"

    @classmethod
    def scan_station(cls, station: str, start: float, end: float) -> typing.Dict[str, typing.List["Instrument"]]:
        def get_path(root: variant.Metadata, *path) -> typing.Optional[typing.Any]:
            for p in path:
                root = root.get(p)
                if not root:
                    return None
            return root

        result: typing.Dict[str, typing.List["Instrument"]] = dict()
        for identity, value, _ in read_archive([Selection(
                start=start,
                end=end,
                stations=[station],
                archives=["raw_meta"],
                include_meta_archive=False,
                include_default_station=False,
        )]):
            if identity.variable == "alias":
                continue
            if not isinstance(value, variant.Metadata):
                continue
            parts = identity.variable.split('_', 1)
            if len(parts) != 2:
                continue

            instrument_id = get_path(value, "Source", "Name")
            if not instrument_id:
                instrument_id = parts[1]
            instrument_id = str(instrument_id)

            source_identifier = cls.Identifier()
            source_identifier.cpd3_component = get_path(value, "Source", "Component")
            source_identifier.forge_instrument = get_path(value, "Source", "ForgeInstrument")

            identifier: typing.Dict[str, typing.Any] = {
                key: get_path(value, "Source", key) for key in ("Manufacturer", "Model", "SerialNumber")
            }

            instrument_segments = result.get(instrument_id)
            if not instrument_segments:
                instrument_segments = list()
                result[instrument_id] = instrument_segments
            else:
                # Ignore state metadata, since it can be persisted longer than it should
                smoothing_mode = get_path(value, "Smoothing", "Mode")
                if smoothing_mode:
                    smoothing_mode = str(smoothing_mode).lower()
                    if smoothing_mode == "none" or smoothing_mode == "bypass":
                        continue

            effective_start = max(start, identity.start) if identity.start else start
            effective_end = min(end, identity.end) if identity.end else end

            def can_merge(target: Instrument) -> bool:
                if target.source != source_identifier:
                    return False
                if effective_end - effective_start < (2 * 60 * 60):
                    return True
                effective_start_day = int(floor(effective_start / (24 * 60 * 60)))
                target_end_day = int(ceil(target.end / (24 * 60 * 60)))
                if effective_start_day < target_end_day + 1:
                    return True
                # Since the analysis is about conversion type, ignore gaps as long as the identifier matches
                return target._last_identifier == identifier

            if len(instrument_segments) == 0 or not can_merge(instrument_segments[-1]):
                if len(instrument_segments) >= 1:
                    instrument_segments[-1].end = min(instrument_segments[-1].end, effective_start)

                instrument_data = cls()
                instrument_data.start = effective_start
                instrument_data.end = effective_end
                instrument_segments.append(instrument_data)
            else:
                instrument_data = instrument_segments[-1]
                instrument_data.end = max(instrument_data.end, effective_end)

            if source_identifier:
                instrument_data.source = source_identifier
            instrument_data.variables.add(identity.variable)
            instrument_data._last_identifier = identifier

        return result


def main():
    parser = argparse.ArgumentParser(description=f"CPD3 raw data analysis summary")
    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--start',
                        dest='start',
                        help="override start time")
    parser.add_argument('--end',
                        dest='end',
                        help="override end time")
    parser.add_argument('--variables',
                        dest='variables', action='store_true',
                        help="show constituent variables")
    parser.add_argument('station',
                        help="station to analyze")
    args = parser.parse_args()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    station = args.station.lower()
    assert station in VALID_STATIONS
    start_time = parse_time_argument(args.start).timestamp() if args.start else station_data(station, 'legacy', 'DATA_START_TIME')
    end_time = parse_time_argument(args.end).timestamp() if args.end else station_data(station, 'legacy', 'DATA_END_TIME')

    _LOGGER.debug(f"Starting raw data analysis for {station.upper()} in {format_iso8601_time(start_time)} to {format_iso8601_time(end_time)}")
    begin_time = time.monotonic()
    available = Instrument.scan_station(station, start_time, end_time)
    end_time = time.monotonic()
    _LOGGER.debug(f"Analysis of {len(available)} instruments completed in {(end_time - begin_time):.2f} seconds")

    for instrument_id in sorted(available.keys()):
        segments = available[instrument_id]
        print(instrument_id)
        for segment in segments:
            print(f"  {format_iso8601_time(segment.start)} {format_iso8601_time(segment.end)} - {str(segment.source)}")
            if args.variables:
                print(f"    {', '.join(sorted(segment.variables))}")


if __name__ == '__main__':
    main()