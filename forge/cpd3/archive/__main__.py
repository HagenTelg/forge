#!/usr/bin/env python3

import typing
import logging
import asyncio
import argparse
import sys
import time
from math import floor, ceil, inf
from tempfile import NamedTemporaryFile
from netCDF4 import Dataset
from forge.timeparse import parse_iso8601_time
from forge.logicaltime import containing_year_range, start_of_year
from forge.range import intersects
from forge.archive.client.connection import Connection, LockDenied, LockBackoff
from forge.archive.client import event_log_lock_key, event_log_file_name, passed_lock_key, passed_file_name
from forge.cpd3.identity import Identity, Name
from forge.cpd3.datawriter import StandardDataOutput, serialize_archive_value
from forge.cpd3.archive.selection import Selection as CPD3Selection, FileMatch
from forge.cpd3.archive.lookup import IndexLookup
from forge.cpd3.convert.station.lookup import station_data

_LOGGER = logging.getLogger(__name__)


class _DataWriter(StandardDataOutput):
    def __init__(self):
        super().__init__()
        sys.stdout.buffer.write(self.RAW_HEADER)

    def output_ready(self, packet: bytes) -> None:
        sys.stdout.buffer.write(self.raw_encode(packet))


def _selection_time_range(selections: typing.List["CPD3Selection"]) -> typing.Tuple[float, float]:
    start: typing.Optional[float] = selections[0].start
    end: typing.Optional[float] = selections[0].end
    for sel in selections[1:]:
        if not sel.start or (start and start > sel.start):
            start = sel.start
        if not sel.end or (end and end < sel.end):
            end = sel.end
    if start is None:
        start = 1.0
    if end is None:
        end = time.time()
    return start, end


def _filter_for_selections(station: str, archive: str, selections: typing.List["CPD3Selection"],
                           data: typing.List[typing.Tuple]) -> typing.List[typing.Tuple]:
    compiled_selections: typing.List[FileMatch] = list()
    for sel in selections:
        compiled_selections.append(FileMatch(sel, station, archive))

    verdict: typing.Dict[Name, typing.Union[bool, typing.Tuple[float, float]]] = dict()
    result: typing.List[typing.Tuple] = list()
    for v in data:
        ident: Identity = v[0]
        outcome = verdict.get(ident.name)
        if outcome is None:
            outcome = False
            accept_start: typing.Optional[float] = None
            accept_end: typing.Optional[float] = None
            for sel in compiled_selections:
                if not sel.accept_name(ident.name):
                    continue
                if outcome is False:
                    accept_start = sel.start
                    accept_end = sel.end
                else:
                    if accept_start is not None and (sel.start is None or sel.start < accept_start):
                        accept_start = sel.start
                    if accept_end is not None and (sel.end is None or sel.end > accept_end):
                        accept_end = sel.end
            if outcome is not False:
                outcome = (accept_start, accept_end)
            verdict[ident.name] = outcome
        if outcome is False:
            continue
        accept_start, accept_end = outcome
        if not intersects(
                accept_start if accept_start is not None else -inf,
                accept_end if accept_end is not None else inf,
                ident.start if ident.start is not None else -inf,
                ident.end if ident.end is not None else inf,
        ):
            continue
        result.append(v)
    return result


async def _read_events(
        connection: Connection,
        selections: typing.Dict[str, typing.List["CPD3Selection"]],
) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    result: typing.List[typing.Tuple[Identity, typing.Any]] = list()
    for station, station_selections in selections.items():
        read_start, read_end = _selection_time_range(station_selections)
        await connection.lock_read(
            event_log_lock_key(station),
            int(floor(read_start * 1000)),
            int(ceil(read_end * 1000))
        )
        day_start = int(floor(read_start / (24 * 60 * 60))) * 24 * 60 * 60
        day_end = int(ceil(read_end / (24 * 60 * 60))) * 24 * 60 * 60

        converter = station_data(station, 'archive', 'event_log')

        add_data: typing.List[typing.Tuple[Identity, typing.Any]] = list()
        for file_start_time in range(day_start, day_end, 24 * 60 * 60):
            with NamedTemporaryFile(suffix=".nc") as data_file:
                try:
                    await connection.read_file(event_log_file_name(station, file_start_time), data_file)
                except FileNotFoundError:
                    continue
                data_file.flush()
                data = Dataset(data_file.name, 'r')
                try:
                    add_data.extend(converter(station, data))
                finally:
                    data.close()
        result.extend(_filter_for_selections(station, 'events', station_selections, add_data))
    return result


async def _read_passed(
        connection: Connection,
        selections: typing.Dict[str, typing.List["CPD3Selection"]],
) -> typing.List[typing.Tuple[Identity, typing.Any, float]]:
    result: typing.List[typing.Tuple[Identity, typing.Any, float]] = list()
    for station, station_selections in selections.items():
        read_start, read_end = _selection_time_range(station_selections)
        await connection.lock_read(
            passed_lock_key(station),
            int(floor(read_start * 1000)),
            int(ceil(read_end * 1000))
        )

        converter = station_data(station, 'archive', 'data_passed')

        add_data: typing.List[typing.Tuple[Identity, typing.Any]] = list()
        for year in range(*containing_year_range(read_start, read_end)):
            with NamedTemporaryFile(suffix=".nc") as data_file:
                try:
                    await connection.read_file(passed_file_name(station, start_of_year(year)), data_file)
                except FileNotFoundError:
                    continue
                data_file.flush()
                data = Dataset(data_file.name, 'r')
                try:
                    add_data.extend(converter(station, data))
                finally:
                    data.close()
        result.extend(_filter_for_selections(station, 'events', station_selections, add_data))
    return result


def main():
    parser = argparse.ArgumentParser(description="CPD3 archive read loopback interface")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--stream-format',
                        dest='stream_format',
                        help="output CPD3 stream format instead of direct active serialization")

    args = parser.parse_args()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    raw_selections = bytearray(sys.stdin.buffer.read())
    cpd3_selections: typing.List[CPD3Selection] = list()
    while raw_selections:
        cpd3_selections.append(CPD3Selection.deserialize(raw_selections))
    raw_selections = None
    if not cpd3_selections:
        parser.error("No selections on STDIN")
    _LOGGER.debug("Read %d CPD3 archive selections", len(cpd3_selections))

    if args.stream_format:
        data_writer = _DataWriter()
    else:
        data_writer = None

    forge_sources = CPD3Selection.split_sources(cpd3_selections)
    if not forge_sources:
        _LOGGER.debug("No Forge sources for CPD3 selections")
        return

    event_sources: typing.Dict[str, typing.List[CPD3Selection]] = dict()
    passed_sources: typing.Dict[str, typing.List[CPD3Selection]] = dict()
    for station in list(forge_sources.keys()):
        archive_selections = forge_sources[station]
        station_events = archive_selections.pop('events', None)
        station_passed = archive_selections.pop('passed', None)
        if not archive_selections:
            del forge_sources[station]

        if station_events:
            event_sources[station] = station_passed
        if station_passed:
            passed_sources[station] = station_passed

    if event_sources:
        if passed_sources or forge_sources:
            _LOGGER.warning("Event selection contains non-event sources that will be ignored")
            passed_sources.clear()
            forge_sources.clear()
    elif passed_sources:
        if forge_sources:
            _LOGGER.warning("Passed selection contains non-event sources that will be ignored")
            forge_sources.clear()

    loop = asyncio.new_event_loop()

    async def run():
        async with await Connection.default_connection("CPD3 archive read") as connection:
            backoff = LockBackoff()
            while True:
                try:
                    async with connection.transaction():
                        if event_sources:
                            output_data: typing.List[typing.Tuple[Identity, typing.Any]] = await _read_events(connection, event_sources)
                            output_data.sort(key=lambda x: x[0].start)
                            if data_writer:
                                for ident, value, _ in output_data:
                                    data_writer.incoming_value(ident, value)
                            else:
                                for ident, value in output_data:
                                    sys.stdout.buffer.write(serialize_archive_value(ident, value, ident.start))
                            break
                        elif passed_sources:
                            output_data: typing.List[typing.Tuple[Identity, typing.Any, float]] = await _read_passed(connection, passed_sources)
                            output_data.sort(key=lambda x: x[0].start)
                            if data_writer:
                                for ident, value, _ in output_data:
                                    data_writer.incoming_value(ident, value)
                            else:
                                for ident, value, modified in output_data:
                                    sys.stdout.buffer.write(serialize_archive_value(ident, value, modified))
                            break

                        lookup = IndexLookup(forge_sources)
                        await lookup.integrate_index(connection)
                        await lookup.acquire_locks(connection)

                        clip_start: float = -inf
                        async for file_data in lookup.files(connection):
                            output_data: typing.List[typing.Tuple[Identity, typing.Any]] = list()
                            latest_modified: typing.Optional[float] = None
                            for file, possible_match, station, archive in file_data:
                                size_before = len(output_data)
                                station_data(station, 'archive', 'convert')(
                                    station, archive, file, possible_match, output_data
                                )
                                if size_before != len(output_data):
                                    file_creation_time = getattr(file, 'date_created', None)
                                    if file_creation_time is not None:
                                        file_creation_time: float = parse_iso8601_time(str(file_creation_time)).timestamp()
                                        if not latest_modified or file_creation_time > latest_modified:
                                            latest_modified = file_creation_time

                            output_data.sort(key=lambda x: x[0].start)

                            # Resumed state values should already be hit, so discard anything out of order
                            while output_data and output_data[0][0].start < clip_start:
                                del output_data[0]
                            if output_data:
                                clip_start = output_data[-1][0].start

                            if data_writer:
                                for ident, value, _ in output_data:
                                    data_writer.incoming_value(ident, value)
                            else:
                                if latest_modified is None:
                                    latest_modified = time.time()
                                for ident, value in output_data:
                                    sys.stdout.buffer.write(serialize_archive_value(ident, value, latest_modified))
                    break
                except LockDenied as ld:
                    _LOGGER.debug("Archive busy: %s", ld.status)
                    await backoff()
                    continue

    loop.run_until_complete(run())
    loop.close()

    if data_writer:
        data_writer.finish()


if __name__ == '__main__':
    main()
