import typing
import asyncio
import logging
import time
from math import floor, ceil
from json import loads as from_json, dumps as to_json
from netCDF4 import Dataset
from forge.timeparse import parse_iso8601_time
from forge.logicaltime import start_of_year_ms

_LOGGER = logging.getLogger(__name__)


class InstrumentHistory:
    HISTORY_VERSION = 1

    def __init__(self, json_data: typing.Optional[bytes] = None):
        if not json_data:
            self.tags: typing.Dict[str, typing.List[typing.Optional[typing.Set[str]]]] = dict()
            self.instrument_code: typing.Dict[str, typing.List[typing.Optional[str]]] = dict()
            self.manufacturer: typing.Dict[str, typing.List[typing.Optional[str]]] = dict()
            self.model: typing.Dict[str, typing.List[typing.Optional[str]]] = dict()
            self.serial_number: typing.Dict[str, typing.List[typing.Optional[str]]] = dict()
            return

        contents = from_json(json_data)
        try:
            version = contents['version']
        except KeyError:
            raise RuntimeError("No history version available")
        if version != self.HISTORY_VERSION:
            raise RuntimeError(f"History version mismatch ({version} vs {self.HISTORY_VERSION})")

        self.tags: typing.Dict[str, typing.List[typing.Optional[typing.Set[str]]]] = {
            instrument_id: [set(day) if day is not None else None for day in raw_tags]
            for instrument_id, raw_tags in contents['tags'].items()
        }
        self.instrument_code: typing.Dict[str, typing.List[typing.Optional[str]]] = contents['instrument']
        self.manufacturer: typing.Dict[str, typing.List[typing.Optional[str]]] = contents['manufacturer']
        self.model: typing.Dict[str, typing.List[typing.Optional[str]]] = contents['model']
        self.serial_number: typing.Dict[str, typing.List[typing.Optional[str]]] = contents['serial_number']

    @property
    def known_instrument_ids(self) -> typing.Iterable[str]:
        return self.tags.keys()

    def instrument(self, instrument_id: str) -> typing.Dict[str, typing.List[typing.Optional[str]]]:
        result: typing.Dict[str, typing.List[typing.Optional[str]]] = dict()

        result['instrument'] = self.instrument_code.get(instrument_id, [])
        result['manufacturer'] = self.manufacturer.get(instrument_id, [])
        result['model'] = self.model.get(instrument_id, [])
        result['serial_number'] = self.serial_number.get(instrument_id, [])

        max_length = 0
        for add in result.values():
            max_length = max(max_length, len(add))
        for add in result.values():
            while len(add) < max_length:
                add.append(None)

        return result

    def integrate_existing(self, contents: bytes) -> None:
        if not contents:
            return
        contents = from_json(contents)
        try:
            version = contents['version']
        except KeyError:
            raise RuntimeError("No history version available")
        if version != self.HISTORY_VERSION:
            raise RuntimeError(f"History version mismatch ({version} vs {self.HISTORY_VERSION})")

        tags: typing.Dict[str, typing.List[typing.Optional[typing.Set[str]]]] = {
            instrument_id: [set(day) if day is not None else None for day in raw_tags]
            for instrument_id, raw_tags in contents['tags'].items()
        }
        instrument_code: typing.Dict[str, typing.List[typing.Optional[str]]] = contents['instrument']
        manufacturer: typing.Dict[str, typing.List[typing.Optional[str]]] = contents['manufacturer']
        model: typing.Dict[str, typing.List[typing.Optional[str]]] = contents['model']
        serial_number: typing.Dict[str, typing.List[typing.Optional[str]]] = contents['serial_number']

        def underlay(destination: typing.Dict[str, typing.List], under: typing.Dict[str, typing.List]) -> None:
            for instrument_id, under_values in under.items():
                existing = destination.get(instrument_id)
                if not existing:
                    destination[instrument_id] = under_values
                    continue

                if len(existing) < len(under_values):
                    existing.extend([None] * (len(under_values) - len(existing)))
                for i in range(len(under_values)):
                    if existing[i] is not None:
                        continue
                    existing[i] = under_values[i]

        underlay(self.tags, tags)
        underlay(self.instrument_code, instrument_code)
        underlay(self.manufacturer, manufacturer)
        underlay(self.model, model)
        underlay(self.serial_number, serial_number)

    def update_file(self, file: Dataset, file_start_ms: int = None, file_end_ms: int = None) -> None:
        instrument_id = file.instrument_id

        if file_start_ms is None:
            time_coverage_start = getattr(file, 'time_coverage_start', None)
            if time_coverage_start is not None:
                file_start_ms = int(floor(parse_iso8601_time(str(time_coverage_start)).timestamp() * 1000.0))
        if file_end_ms is None:
            time_coverage_end = getattr(file, 'time_coverage_end', None)
            if time_coverage_end is not None:
                file_end_ms = int(ceil(parse_iso8601_time(str(time_coverage_end)).timestamp() * 1000.0))

        assert file_start_ms is not None
        assert file_end_ms is not None
        assert file_start_ms < file_end_ms

        start_year_number = time.gmtime(file_start_ms / 1000.0).tm_year
        offset = start_of_year_ms(start_year_number)

        file_start_day = int(floor(file_start_ms - offset) / (24 * 60 * 60 * 1000))
        file_end_day = int(ceil(file_start_ms - offset) / (24 * 60 * 60 * 1000))
        if file_end_day <= file_start_day:
            file_end_day = file_start_day + 1

        tags = set(getattr(file, 'forge_tags', '').split())
        instrument_code = getattr(file, 'instrument', None)

        manufacturer = None
        model = None
        serial_number = None
        instrument_group = file.groups.get('instrument')
        if instrument_group is not None:
            v = instrument_group.variables.get('manufacturer')
            if v is not None:
                v = str(v[0])
                if v:
                    manufacturer = v

            v = instrument_group.variables.get('model')
            if v is not None:
                v = str(v[0])
                if v:
                    model = v

            v = instrument_group.variables.get('serial_number')
            if v is not None:
                v = str(v[0])
                if v:
                    serial_number = v

        def acquire_target(lookup: typing.Dict[str, typing.List]) -> typing.List:
            v = lookup.get(instrument_id)
            if not v:
                v = [None] * file_end_day
                lookup[instrument_id] = v
                return v
            if len(v) < file_end_day:
                v.extend([None] * (file_end_day - len(v)))
            return v

        dest_tags = acquire_target(self.tags)
        dest_instrument_code = acquire_target(self.instrument_code)
        dest_manufacturer = acquire_target(self.manufacturer)
        dest_model = acquire_target(self.model)
        dest_serial_number = acquire_target(self.serial_number)

        for day in range(file_start_day, file_end_day):
            dest_tags[day] = tags
            dest_instrument_code[day] = instrument_code
            dest_manufacturer[day] = manufacturer
            dest_model[day] = model
            dest_serial_number[day] = serial_number

    def commit(self) -> bytes:
        result = {
            'version': self.HISTORY_VERSION,
            'instrument': self.instrument_code,
            'manufacturer': self.manufacturer,
            'model': self.model,
            'serial_number': self.serial_number,
        }

        tags: typing.Dict[str, typing.List[typing.List[str]]] = dict()
        result['tags'] = tags
        for instrument_id, day_tags in self.tags.items():
            instrument_tags = []
            tags[instrument_id] = instrument_tags
            for day in day_tags:
                instrument_tags.append(sorted(day) if day is not None else None)

        return to_json(result, sort_keys=True).encode('ascii')


def cli():
    import argparse
    import sys
    import re
    import datetime
    from forge.const import STATIONS
    from forge.timeparse import parse_time_bounds_arguments
    from forge.logicaltime import containing_year_range, start_of_year
    from forge.archive import CONFIGURATION
    from forge.archive.client import index_lock_key, index_instrument_history_file_name
    from forge.archive.client.connection import Connection, LockDenied, LockBackoff

    parser = argparse.ArgumentParser(description="Forge instrument history summary.")

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

    parser.add_argument('--csv',
                        dest='csv', action='store_true',
                        help="output in CSV format")

    parser.add_argument('--tags',
                        dest='tags', nargs='*',
                        help="tags to match")
    parser.add_argument('--instrument',
                        dest='instruments', nargs='*',
                        help="instruments to match")
    parser.add_argument('--instrument-code',
                        dest='instrument_codes', nargs='*',
                        help="instruments type codes to match")

    parser.add_argument('station', help="station code")
    parser.add_argument('time', help="time bounds to display", nargs='*')

    args = parser.parse_args()
    if args.tcp_server and not args.tcp_port:
        parser.error("Both a server host and port must be specified")

    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    station = args.station.lower()
    if station not in STATIONS:
        parser.error("Invalid station code")
    if args.time:
        start, end = parse_time_bounds_arguments(args.time)
        start = start.timestamp()
        end = end.timestamp()
    else:
        start = 1
        end = time.time()

    start_year, end_year = containing_year_range(start, end)

    tags_match = []
    instrument_match = []
    instrument_code_match = []
    def split_strip(v):
        result = []
        for add in re.split(r"[\s:;,]+", v):
            add = add.strip()
            if not add:
                continue
            result.append(add)
        return result

    if args.tags:
        for add in args.tags:
            tags_match.extend(split_strip(add))
    if args.instruments:
        for add in args.instruments:
            instrument_match.extend(split_strip(add))
    if args.instrument_codes:
        for add in args.instrument_codes:
            instrument_code_match.extend(split_strip(add))

    loop = asyncio.new_event_loop()

    class InstrumentPoint:
        def __init__(self, epoch: int,
                     tags: typing.Optional[typing.Set[str]],
                     instrument_code: typing.Optional[str] = None,
                     manufacturer: typing.Optional[str] = None,
                     model: typing.Optional[str] = None,
                     serial_number: typing.Optional[str] = None):
            self.epoch = epoch
            self.tags = tags
            self.instrument_code = instrument_code
            self.manufacturer = manufacturer
            self.model = model
            self.serial_number = serial_number

        @property
        def exists(self) -> bool:
            return self.tags is not None

        @property
        def passes_filter(self) -> bool:
            if tags_match:
                if self.tags is None:
                    return False
                for tag in tags_match:
                    if tag.startswith("-"):
                        if tag[1:] in self.tags:
                            return False
                    else:
                        if tag not in self.tags:
                            return False

            if instrument_code_match:
                any_match = False
                for code in instrument_code_match:
                    if code.startswith("-"):
                        if not self.instrument_code:
                            continue
                        if code[1:].lower() == self.instrument_code.lower():
                            return False
                    else:
                        if not self.instrument_code:
                            return False
                        any_match = True
                        if code.lower() == self.instrument_code.lower():
                            break
                else:
                    if any_match:
                        return False

            return True

        def __eq__(self, other):
            if not isinstance(other, InstrumentPoint):
                return False
            return self.instrument_code == other.instrument_code and \
                self.manufacturer == other.manufacturer and self.model == other.model and \
                self.serial_number == other.serial_number

        @classmethod
        def from_history(
                cls,
                start_of_year: int,
                tags: typing.List[typing.Optional[typing.Set[str]]],
                instrument_code: typing.Optional[typing.List[typing.Optional[str]]],
                manufacturer: typing.Optional[typing.List[typing.Optional[str]]],
                model: typing.Optional[typing.List[typing.Optional[str]]],
                serial_number: typing.Optional[typing.List[typing.Optional[str]]],
        ) -> typing.List["InstrumentPoint"]:
            result: typing.List["InstrumentPoint"] = list()

            for doy in range(len(tags)):
                epoch = start_of_year + doy * (24 * 60 * 60)
                point = InstrumentPoint(
                    epoch, tags[doy],
                    instrument_code[doy] if instrument_code and doy < len(instrument_code) else None,
                    manufacturer[doy] if manufacturer and doy < len(manufacturer) else None,
                    model[doy] if model and doy < len(model) else None,
                    serial_number[doy] if serial_number and doy < len(serial_number) else None,
                )
                if not point.passes_filter:
                    result.append(InstrumentPoint(epoch, None))
                    continue
                result.append(point)

            return result

        @staticmethod
        def deduplicate(points: typing.List["InstrumentPoint"]) -> None:
            for i in reversed(range(1, len(points))):
                if points[i] == points[i - 1]:
                    del points[i]

    history_points: typing.Dict[str, typing.List[InstrumentPoint]] = dict()

    async def run():
        if args.tcp_server and args.tcp_port:
            _LOGGER.debug(f"Connecting to archive TCP socket {args.tcp_server}:{args.tcp_port}")
            reader, writer = await asyncio.open_connection(args.tcp_server, int(args.tcp_port))
            connection = Connection(reader, writer, "instrument history")
        elif args.unix_socket:
            _LOGGER.debug(f"Connecting to archive Unix socket {args.unix_socket}")
            reader, writer = await asyncio.open_unix_connection(args.unix_socket)
            connection = Connection(reader, writer, "instrument history")
        else:
            connection = await Connection.default_connection("instrument history")

        await connection.startup()

        async def read_history(year: int) -> None:
            start = start_of_year(year)
            try:
                history_contents = await connection.read_bytes(index_instrument_history_file_name(
                    station, "raw", start
                ))
            except FileNotFoundError:
                _LOGGER.debug("No history for %d found", year)
                return
            _LOGGER.debug("Read %d bytes of history for %d", len(history_contents), year)
            history = InstrumentHistory(history_contents)

            def integrate_instrument(instrument_id: str):
                if instrument_match:
                    any_match = False
                    for check_id in instrument_match:
                        if check_id.startswith("-"):
                            if check_id[1:] == instrument_id:
                                return
                        else:
                            any_match = True
                            if check_id == instrument_id:
                                break
                    else:
                        if any_match:
                            return

                update = InstrumentPoint.from_history(
                    start,
                    history.tags[instrument_id],
                    history.instrument_code.get(instrument_id),
                    history.manufacturer.get(instrument_id),
                    history.model.get(instrument_id),
                    history.serial_number.get(instrument_id),
                )

                target = history_points.get(instrument_id)
                if not target:
                    target = []
                    history_points[instrument_id] = target
                target.extend(update)

            for instrument_id in history.known_instrument_ids:
                integrate_instrument(instrument_id)

        backoff = LockBackoff()
        try:
            while True:
                history_points.clear()
                try:
                    async with connection.transaction():
                        await connection.lock_read(
                            index_lock_key(station, "raw"), int(floor(start * 1000)), int(ceil(end * 1000))
                        )
                        for year in range(start_year, end_year):
                            await read_history(year)
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

    latest_instruments: typing.Dict[str, InstrumentPoint] = dict()
    for instrument_id, points in history_points.items():
        points.sort(key=lambda p: p.epoch)

        while points and not points[0].exists:
            del points[0]
        if not points:
            continue

        latest_point = points[-1]
        if latest_point.exists:
            latest_instruments[instrument_id] = points[-1]

        InstrumentPoint.deduplicate(points)

    combined: typing.List[typing.Tuple[str, InstrumentPoint]] = list()
    for instrument_id, points in history_points.items():
        for point in points:
            combined.append((instrument_id, point))
    combined.sort(key=lambda p: (p[1].epoch, p[0]))

    if not combined:
        if sys.stdout.isatty():
            sys.stdout.write("No instrument history available\n")
        exit(1)

    timeline_rows: typing.List[typing.List[str]] = [[
        "DATE UTC",
        "ID",
        "TYPE",
        "MANUFACTURER",
        "MODEL",
        "SERIAL NUMBER",
    ]]
    for (instrument_id, point) in combined:
        ts = time.gmtime(point.epoch)
        if not point.exists:
            timeline_rows.append([
                f"{ts.tm_year:04}-{ts.tm_mon:02}-{ts.tm_mday:02}",
                instrument_id,
                "REMOVED",
            ])
            continue
        timeline_rows.append([
            f"{ts.tm_year:04}-{ts.tm_mon:02}-{ts.tm_mday:02}",
            instrument_id,
            point.instrument_code or "",
            point.manufacturer or "",
            point.model or "",
            point.serial_number or "",
        ])

    if args.csv:
        import csv
        writer = csv.writer(sys.stdout)
        for row in timeline_rows:
            writer.writerow(row)
        return

    timeline_columns_widths = [0] * len(timeline_rows[0])
    for row in timeline_rows:
        for col_idx in range(len(row)):
            timeline_columns_widths[col_idx] = max(timeline_columns_widths[col_idx], len(row[col_idx]) + 1)

    for row in timeline_rows:
        line = ""
        for col_idx in range(len(row)):
            line += row[col_idx].ljust(timeline_columns_widths[col_idx])
        print(line)

    if not latest_instruments:
        return

    print("")
    last_epoch = max([p.epoch for p in latest_instruments.values() if p.exists])
    days = int(floor((time.time() - last_epoch) / (24 * 60 * 60)))

    if days > 1:
        ts = time.gmtime(last_epoch)
        print(f"LATEST ({ts.tm_year:04}-{ts.tm_mon:02}-{ts.tm_mday:02} = {days} days ago):")
    elif days == 1:
        ts = time.gmtime(last_epoch)
        print(f"LATEST ({ts.tm_year:04}-{ts.tm_mon:02}-{ts.tm_mday:02} = 1 day ago):")
    else:
        print(f"CURRENT:")
    for instrument_id in sorted(latest_instruments.keys()):
        point = latest_instruments[instrument_id]
        if last_epoch - point.epoch > 1 * 24 * 60 * 60:
            continue
        row = [
            "",
            instrument_id,
            point.instrument_code or "",
            point.manufacturer or "",
            point.model or "",
            point.serial_number or "",
        ]
        line = ""
        for col_idx in range(len(row)):
            line += row[col_idx].ljust(timeline_columns_widths[col_idx])
        print(line)


if __name__ == '__main__':
    cli()