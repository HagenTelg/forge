import typing
import asyncio
import logging
import argparse
import time
from math import floor, ceil
from netCDF4 import Dataset
from importlib import import_module
from tempfile import NamedTemporaryFile
from forge.const import STATIONS as VALID_STATIONS
from forge.formattime import format_iso8601_time
from forge.timeparse import parse_time_argument
from forge.archive.client.connection import Connection, LockBackoff, LockDenied
from forge.archive.client.put import ArchivePut
from forge.cpd3.convert.station.lookup import station_data
from forge.cpd3.legacy.instrument.converter import InstrumentConverter

_LOGGER = logging.getLogger(__name__)



class InstrumentTimeConversion:
    def __init__(self, converter: typing.Union[str, typing.Type[InstrumentConverter]],
                 start: typing.Optional[typing.Union[float, str]] = None,
                 end: typing.Optional[typing.Union[float, str]] = None):
        if isinstance(converter, str):
            if '.' not in converter:
                converter = import_module('.' + converter, 'forge.cpd3.legacy.instrument').Converter
            else:
                converter = import_module('.', converter).Converter
        self.converter: typing.Type[InstrumentConverter] = converter

        if isinstance(start, str):
            start = parse_time_argument(start).timestamp()
        self.start: float = start
        if isinstance(end, str):
            end = parse_time_argument(end).timestamp()
        self.end: float = end

    @staticmethod
    def run(station: str, converters: typing.Dict[str, typing.List["InstrumentTimeConversion"]],
            start: typing.Optional[float] = None, end: typing.Optional[float] = None) -> None:
        assert station in VALID_STATIONS

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
        parser.add_argument('--show-missing',
                            dest='show_missing', action='store_true',
                            help="show missing conversions instead of converting data")
        parser.add_argument('--instrument',
                            dest='instrument',
                            help="instrument ID restriction")
        args = parser.parse_args()
        if args.debug:
            from forge.log import set_debug_logger
            set_debug_logger()

        if not start or args.start:
            start = parse_time_argument(args.start).timestamp() if args.start else station_data(station, 'legacy','DATA_START_TIME')
        if not end or args.end:
            end = parse_time_argument(args.end).timestamp() if args.end else station_data(station, 'legacy','DATA_END_TIME')
        assert start < end

        for defined_segments in converters.values():
            for segment in defined_segments:
                if not segment.start:
                    segment.start = start
                if not segment.end:
                    segment.end = end
            defined_segments.sort(key=lambda x: x.start)
            while defined_segments and defined_segments[0].start < start:
                defined_segments[0].start = start
                if defined_segments[0].start >= defined_segments[0].end:
                    del defined_segments[0]
            while defined_segments and defined_segments[-1].end > end:
                defined_segments[-1].end = end
                if defined_segments[-1].start >= defined_segments[-1].end:
                    del defined_segments[-1]
            for idx in range(len(defined_segments)-1):
                assert defined_segments[idx].end <= defined_segments[idx+1].start

        if args.show_missing:
            from forge.cpd3.legacy.raw.analyze import Instrument

            _LOGGER.debug(f"Starting raw data analysis for {station.upper()} in {format_iso8601_time(start)} to {format_iso8601_time(end)}")
            begin_time = time.monotonic()
            available = Instrument.scan_station(station, start, end)
            end_time = time.monotonic()
            _LOGGER.debug(f"Analysis of {len(available)} instruments completed in {(end_time - begin_time):.2f} seconds")

            from forge.range import Subtractor

            for instrument_id in list(available.keys()):
                available_segments = available[instrument_id]
                while available_segments and available_segments[0].start < start:
                    available_segments[0].start = start
                    if available_segments[0].start >= available_segments[0].end:
                        del available_segments[0]
                while available_segments and available_segments[-1].end > end:
                    available_segments[-1].end = end
                    if available_segments[-1].start >= available_segments[-1].end:
                        del available_segments[-1]
                if not available:
                    continue

                defined_converters = converters.get(instrument_id)
                if not defined_converters:
                    continue
                available_segments = list(available_segments)
                defined_converters = list(defined_converters)

                class AvailableSubtract(Subtractor):
                    @property
                    def canonical(self) -> bool:
                        return True

                    def __len__(self) -> int:
                        return len(available_segments)

                    def __delitem__(self, key: int) -> None:
                        del available_segments[key]

                    def get_start(self, index: int) -> typing.Union[int, float]:
                        return available_segments[index].start

                    def get_end(self, index: int) -> typing.Union[int, float]:
                        return available_segments[index].end

                    def set_start(self, index: int, value: typing.Union[int, float]) -> None:
                        available_segments[index].start = value

                    def set_end(self, index: int, value: typing.Union[int, float]) -> None:
                        available_segments[index].end = value

                    def duplicate_after(self, source: int, start: typing.Union[int, float],
                                        end: typing.Union[int, float]) -> None:
                        base = available_segments[source]
                        add = Instrument()
                        add.start = start
                        add.end = end
                        add.variables = base.variables
                        add.source = base.source
                        available_segments.insert(source + 1, add)

                subtract = AvailableSubtract()
                for segment in converters[instrument_id]:
                    subtract(segment.start, segment.end)

                class DefinedSubtract(Subtractor):
                    @property
                    def canonical(self) -> bool:
                        return True

                    def __len__(self) -> int:
                        return len(defined_converters)

                    def __delitem__(self, key: int) -> None:
                        del defined_converters[key]

                    def get_start(self, index: int) -> typing.Union[int, float]:
                        return defined_converters[index].start

                    def get_end(self, index: int) -> typing.Union[int, float]:
                        return defined_converters[index].end

                    def set_start(self, index: int, value: typing.Union[int, float]) -> None:
                        defined_converters[index].start = value

                    def set_end(self, index: int, value: typing.Union[int, float]) -> None:
                        defined_converters[index].end = value

                    def duplicate_after(self, source: int, start: typing.Union[int, float],
                                        end: typing.Union[int, float]) -> None:
                        base = defined_converters[source]
                        defined_converters.insert(source + 1, InstrumentTimeConversion(base.converter, start, end))

                subtract = DefinedSubtract()
                for segment in available[instrument_id]:
                    subtract(segment.start, segment.end)

                if available_segments:
                    available[instrument_id] = available_segments
                else:
                    del available[instrument_id]
                if defined_converters:
                    converters[instrument_id] = defined_converters
                else:
                    del converters[instrument_id]

            remaining_instruments = set(available.keys())
            remaining_instruments.update(converters.keys())
            for instrument_id in sorted(remaining_instruments):
                print(instrument_id)
                available_segments = available.get(instrument_id)
                if available_segments:
                    print("  PRESENT ONLY IN DATA:")
                    for segment in available_segments:
                        print(f"    {format_iso8601_time(segment.start)} {format_iso8601_time(segment.end)} - {str(segment.source)}")
                defined_converters = converters.get(instrument_id)
                if defined_converters:
                    print("  PRESENT ONLY IN CONVERSION:")
                    for segment in defined_converters:
                        print(f"    {format_iso8601_time(segment.start)} {format_iso8601_time(segment.end)}")

            return

        begin_time = time.monotonic()
        _LOGGER.info(f"Starting raw data conversion for {station.upper()} in {format_iso8601_time(start)} to {format_iso8601_time(end)}")
        total_files = 0

        async def run():
            nonlocal total_files
            for instrument_id, segments in converters.items():
                if args.instrument and instrument_id != args.instrument:
                    continue
                for conversion in segments:
                    assert conversion.start < conversion.end
                    start_day = int(floor(conversion.start / (24 * 60 * 60))) * 24 * 60 * 60
                    end_day = int(ceil(conversion.end / (24 * 60 * 60))) * 24 * 60 * 60
                    if end_day <= conversion.end:
                        end_day += 24 * 60 * 60
                    assert start_day < end_day

                    for start_of_day in range(start_day, end_day, 24 * 60 * 60):
                        end_of_day = start_of_day + 24 * 60 * 60
                        incomplete_day = False

                        if start_of_day < start:
                            incomplete_day = True
                            start_of_day = start
                        if end_of_day > end:
                            incomplete_day = True
                            end_of_day = end
                        if start_of_day >= end_of_day:
                            continue

                        _LOGGER.debug(f"Converting {instrument_id} day {format_iso8601_time(start_of_day)}")
                        with NamedTemporaryFile(suffix=".nc") as output_file:
                            root = Dataset(output_file.name, 'w', format='NETCDF4')
                            try:
                                if not conversion.converter(station, instrument_id, start_of_day, end_of_day, root).run():
                                    continue
                            finally:
                                root.close()

                            async with (await Connection.default_connection("write legacy raw")) as connection:
                                await write_day(connection, output_file.name, station, start_of_day, end_of_day,
                                                incomplete_day)
                            total_files += 1


        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run())
        loop.close()

        end_time = time.monotonic()
        _LOGGER.info(f"Conversion of {len(converters)} instruments in {total_files} files completed in {(end_time - begin_time):.2f} seconds")


async def write_day(
        connection: Connection,
        filename: str,
        station: str,
        start_of_day: float,
        end_of_day: float,
        incomplete_day: bool = False,
        archive: str = "raw",
) -> None:
    backoff = LockBackoff()
    start_of_day_ms = int(floor(start_of_day * 1000))
    end_of_day_ms = int(ceil(end_of_day * 1000))
    while True:
        try:
            async with connection.transaction(True):
                put = ArchivePut(connection)
                await put.preemptive_lock_range(station, archive, start_of_day_ms, end_of_day_ms)

                root = Dataset(filename, 'r+')
                if incomplete_day:
                    try:
                        await put.data(
                            root, station=station, archive=archive,
                            file_start_ms=start_of_day_ms, file_end_ms=end_of_day_ms,
                        )
                    finally:
                        root.close()
                        root = None
                else:
                    await put.replace_exact(
                        root, station=station, archive=archive,
                        file_start_ms=start_of_day_ms, file_end_ms=end_of_day_ms,
                    )

                await put.commit_index()
            break
        except LockDenied as ld:
            _LOGGER.debug("Archive busy: %s", ld.status)
            await backoff()
