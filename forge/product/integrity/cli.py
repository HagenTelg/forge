import typing
import asyncio
import logging
import argparse
import time
import datetime
import sys
import re
import os
from math import floor, ceil
from pathlib import Path
from tempfile import mkstemp
from concurrent.futures import ProcessPoolExecutor
from forge.const import STATIONS
from forge.timeparse import parse_time_bounds_arguments
from forge.formattime import format_iso8601_time
from forge.temp import WorkingDirectory
from forge.logicaltime import year_bounds_ms
from forge.archive import DEFAULT_ARCHIVE_TCP_PORT
from forge.archive.client.connection import Connection, LockDenied, LockBackoff
from forge.archive.client import data_lock_key, data_file_name
from forge.product.integrity.calculate import calculate_integrity, compare_integrity
from forge.product.integrity.archive import calculate_archive_integrity, apply_output_pattern

_LOGGER = logging.getLogger(__name__)


def _write_integrity(destination: typing.TextIO, file: str,
                     hash_time: float, file_creation_time: typing.Optional[float],
                     file_hash: bytes, data_hash: bytes, qualitative_hash: bytes) -> None:
    destination.write(f"{file},{format_iso8601_time(file_creation_time) if file_creation_time else ''},{format_iso8601_time(hash_time)},{file_hash.hex()},{data_hash.hex()},{qualitative_hash.hex()}\n")


def _write_verify(identifier: str, validity_level: bool, show_identical: bool) -> None:
    if validity_level <= 0:
        if show_identical:
            print(f"{identifier},OK")
    elif validity_level == 1:
        print(f"{identifier},DATA_OK")
    elif validity_level == 2:
        print(f"{identifier},QUALITATIVE_OK")
    else:
        print(f"{identifier},FAILED")


async def _generate_from_files(generate: typing.Iterable[Path], destination: typing.TextIO) -> None:
    hash_time = time.time()
    begin_time = time.monotonic()
    completed_files: int = 0

    with ProcessPoolExecutor() as executor:
        concurrent_limit = max(os.cpu_count() + 2, 32)
        launched_files: typing.Set[asyncio.Future] = set()
        source_files: typing.Dict[asyncio.Future, Path] = dict()

        async def process_launched():
            nonlocal launched_files
            nonlocal source_files
            nonlocal completed_files

            done, pending = await asyncio.wait(launched_files, return_when=asyncio.FIRST_COMPLETED)
            launched_files = set(pending)
            for check in done:
                file = source_files.pop(check)
                try:
                    file_creation_time, file_hash, data_hash, qualitative_hash = check.result()
                except:
                    _LOGGER.error(f"Error processing {file}")
                    raise

                _LOGGER.debug(f"Integrity calculation completed for {file}")
                _write_integrity(destination, str(file), hash_time, file_creation_time, file_hash, data_hash, qualitative_hash)
                completed_files += 1

        async def launch_file(file: Path) -> None:
            nonlocal launched_files
            nonlocal source_files

            _LOGGER.debug(f"Starting integrity calculation on {file}")
            launched = asyncio.get_event_loop().run_in_executor(executor, calculate_integrity,str(file))

            launched_files.add(launched)
            source_files[launched] = file
            while len(launched_files) > concurrent_limit:
                await process_launched()

        async def recurse_dir(dir: Path) -> None:
            subdirs: typing.List[Path] = list()
            for f in dir.iterdir():
                if f.is_dir():
                    subdirs.append(f)
                    continue
                if not f.is_file():
                    continue
                await launch_file(f)
            for dir in subdirs:
                await recurse_dir(dir)

        subdirs: typing.List[Path] = list()
        for f in generate:
            if f.is_dir():
                subdirs.append(f)
                continue
            if not f.is_file():
                continue
            await launch_file(f)
        for dir in subdirs:
            await recurse_dir(dir)

        while launched_files:
            await process_launched()
        executor.shutdown(wait=True)

    _LOGGER.debug(f"Integrity calculation on {completed_files} files completed after {time.monotonic() - begin_time:.2f} seconds")


async def _generate_from_archive(connection: Connection, station: str, archive: str, instrument_match: typing.Set[str],
                                 start_epoch_ms: int, end_epoch_ms: int,
                                 output_file_pattern: typing.Optional[str]) -> None:
    begin_time = time.monotonic()
    completed_files: int = 0

    seen_output_files: typing.Set[str] = set()

    async for archive_file_name, file_creation_time, hash_time, file_hash, data_hash, qualitative_hash in calculate_archive_integrity(
        connection, station, archive, instrument_match, start_epoch_ms, end_epoch_ms,
    ):
        completed_files += 1
        if not output_file_pattern:
            _write_integrity(sys.stdout, archive_file_name, hash_time, file_creation_time, file_hash, data_hash, qualitative_hash)
        else:
            target = apply_output_pattern(output_file_pattern, station, archive, archive_file_name, hash_time, file_creation_time)
            if target not in seen_output_files:
                seen_output_files.add(target)
                with open(target, "wt") as f:
                    _write_integrity(
                        f, archive_file_name, hash_time, file_creation_time,
                        file_hash, data_hash, qualitative_hash
                    )
            else:
                with open(target, "at") as f:
                    _write_integrity(
                        f, archive_file_name, hash_time, file_creation_time,
                        file_hash, data_hash, qualitative_hash
                    )

    _LOGGER.debug(f"Integrity calculation on {completed_files} files completed after {time.monotonic() - begin_time:.2f} seconds")


async def _verify_files(root_directory: Path, verify_input: typing.Iterable[str], show_identical: bool) -> bool:
    begin_time = time.monotonic()
    completed_files: int = 0
    all_matched: bool = True

    with ProcessPoolExecutor() as executor:
        concurrent_limit = max(os.cpu_count() + 2, 32)
        launched_files: typing.Set[asyncio.Future] = set()
        launched_info: typing.Dict[asyncio.Future, str] = dict()

        async def process_launched():
            nonlocal launched_files
            nonlocal launched_info
            nonlocal completed_files
            nonlocal all_matched

            done, pending = await asyncio.wait(launched_files, return_when=asyncio.FIRST_COMPLETED)
            launched_files = set(pending)
            for check in done:
                file = launched_info.pop(check)
                try:
                    validity_level = check.result()
                except:
                    _LOGGER.error(f"Error processing {file}")
                    raise

                _LOGGER.debug(f"Integrity comparison completed for {file}")
                _write_verify(file, validity_level, show_identical)
                if validity_level >= 3:
                    all_matched = False
                completed_files += 1

        async def launch_file(file: Path, identifier: str,
                              file_hash: bytes, data_hash: bytes, qualitative_hash: bytes) -> None:
            nonlocal launched_files
            nonlocal launched_info

            _LOGGER.debug(f"Starting integrity comparison on {identifier}")
            launched = asyncio.get_event_loop().run_in_executor(
                executor, compare_integrity,
                str(file), (file_hash, data_hash, qualitative_hash)
            )

            launched_files.add(launched)
            launched_info[launched] = identifier
            while len(launched_files) > concurrent_limit:
                await process_launched()

        async def process_input(input_file_name: str, file: typing.TextIO) -> None:
            nonlocal all_matched

            line_number: int = 0
            for line in file:
                line_number += 1
                try:
                    (read_file_name, _, _, file_hash, data_hash, qualitative_hash, *_) = line.split(',')
                    file_hash = bytes.fromhex(file_hash)
                    data_hash = bytes.fromhex(data_hash)
                    qualitative_hash = bytes.fromhex(qualitative_hash)
                except ValueError:
                    _LOGGER.warning(f"Malformed line at {input_file_name}:{line_number}")
                    continue

                read_file = root_directory / read_file_name
                if not read_file.exists() or read_file.is_dir():
                    _LOGGER.debug(f"File {read_file} does not exist, skipping")
                    print(f"{read_file_name},MISSING")
                    all_matched = False
                    continue

                await launch_file(read_file, read_file_name, file_hash, data_hash, qualitative_hash)

        for hash_file in verify_input:
            if hash_file == '-':
                await process_input("STDIN", sys.stdin)
            else:
                with open(hash_file, "rt") as read_file:
                    await process_input(hash_file, read_file)

        while launched_files:
            await process_launched()
        executor.shutdown(wait=True)

    _LOGGER.debug(f"Integrity verification on {completed_files} files completed after {time.monotonic() - begin_time:.2f} seconds")

    return all_matched



async def _verify_archive(connection: Connection, verify_input: typing.Iterable[str], show_identical: bool) -> bool:
    begin_time = time.monotonic()
    completed_files: int = 0
    all_matched: bool = True

    archive_file_pattern = re.compile(
        r"(?:^|/)data/(?P<station>[A-Za-z][0-9A-Za-z_]{0,31})/(?P<archive>raw|edited|clean|avgh|avgd|avgm)/(?P<year>\d{4})/"
        r'[A-Z][0-9A-Z_]{0,31}-(?P<instrument>[A-Z][A-Z0-9]*)_'
        r's\d{4}(?P<month>\d{2})(?P<day>\d{2})\.nc$'
    )

    with ProcessPoolExecutor() as executor:
        concurrent_limit = max(os.cpu_count() + 2, 32)
        launched_files: typing.Set[asyncio.Future] = set()
        launched_info: typing.Dict[asyncio.Future, typing.Tuple[str, str]] = dict()

        async with WorkingDirectory() as working_directory:
            async def process_launched():
                nonlocal launched_files
                nonlocal launched_info
                nonlocal completed_files
                nonlocal all_matched

                done, pending = await asyncio.wait(launched_files, return_when=asyncio.FIRST_COMPLETED)
                launched_files = set(pending)
                for check in done:
                    local_file, archive_file_name = launched_info.pop(check)
                    try:
                        validity_level = check.result()
                    except:
                        _LOGGER.error(f"Error processing {archive_file_name}")
                        raise

                    await asyncio.get_event_loop().run_in_executor(None, os.unlink, local_file)

                    _LOGGER.debug(f"Integrity comparison completed for {archive_file_name}")
                    _write_verify(archive_file_name, validity_level, show_identical)
                    if validity_level >= 3:
                        all_matched = False
                    completed_files += 1

            async def launch_file(archive_file_name: str,
                                  file_hash: bytes, data_hash: bytes, qualitative_hash: bytes) -> None:
                nonlocal launched_files
                nonlocal launched_info
                nonlocal all_matched

                fd, local_name = mkstemp(suffix='.nc', dir=working_directory)
                os.close(fd)
                try:
                    with open(local_name, 'wb') as dest:
                        await connection.read_file(archive_file_name, dest)
                except FileNotFoundError:
                    await asyncio.get_event_loop().run_in_executor(None, os.unlink, local_name)
                    print(f"{archive_file_name},MISSING")
                    all_matched = False
                    return

                _LOGGER.debug(f"Starting integrity comparison on {archive_file_name}")
                launched = asyncio.get_event_loop().run_in_executor(
                    executor, compare_integrity,
                    str(local_name), (file_hash, data_hash, qualitative_hash)
                )

                launched_files.add(launched)
                launched_info[launched] = (local_name, archive_file_name)

            async def process_input(input_file_name: str, file: typing.TextIO) -> None:
                line_number: int = 0
                for line in file:
                    line_number += 1
                    try:
                        (read_file_name, _, _, file_hash, data_hash, qualitative_hash, *_) = line.split(',')
                        file_hash = bytes.fromhex(file_hash)
                        data_hash = bytes.fromhex(data_hash)
                        qualitative_hash = bytes.fromhex(qualitative_hash)
                    except ValueError:
                        _LOGGER.warning(f"Malformed line at {input_file_name}:{line_number}")
                        continue
                    m = archive_file_pattern.search(read_file_name)
                    if not m:
                        _LOGGER.warning(f"Invalid archive file at {input_file_name}:{line_number}")
                        continue

                    station = m.group('station').lower()
                    archive = m.group('archive').lower()
                    instrument = m.group('instrument')
                    year = int(m.group('year'))
                    month = int(m.group('month'))
                    day = int(m.group('day'))
                    file_time = datetime.datetime(
                        year, month, day,
                        tzinfo=datetime.timezone.utc
                    ).timestamp()

                    if archive in ("avgd", "avgm"):
                        lock_start_ms, lock_end_ms = year_bounds_ms(year)
                    else:
                        lock_start_ms = int(floor(file_time * 1000))
                        lock_end_ms = lock_start_ms + 24 * 60 * 60 * 1000

                    backoff = LockBackoff()
                    while True:
                        try:
                            async with connection.transaction():
                                await connection.lock_read(data_lock_key(station, archive), lock_start_ms, lock_end_ms)
                                await launch_file(
                                    data_file_name(station, archive, instrument, file_time),
                                    file_hash, data_hash, qualitative_hash
                                )
                            break
                        except LockDenied as ld:
                            _LOGGER.debug("Archive busy: %s", ld.status)
                            await backoff()

                    while len(launched_files) > concurrent_limit:
                        await process_launched()

            for hash_file in verify_input:
                if hash_file == '-':
                    await process_input("STDIN", sys.stdin)
                else:
                    with open(hash_file, "rt") as read_file:
                        await process_input(hash_file, read_file)

            while launched_files:
                await process_launched()

    _LOGGER.debug(f"Integrity verification on {completed_files} files completed after {time.monotonic() - begin_time:.2f} seconds")

    return all_matched


def main():
    parser = argparse.ArgumentParser(description="Forge integrity file control.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--archive-host',
                       dest='archive_tcp_server',
                       help="archive server host")
    group.add_argument('--archive-socket',
                       dest='archive_unix_socket',
                       help="archive_archive server Unix socket")
    parser.add_argument('--archive-port',
                        dest='archive_tcp_port',
                        type=int, default=DEFAULT_ARCHIVE_TCP_PORT,
                        help="archive server port")

    subparsers = parser.add_subparsers(dest='command')

    command_parser = subparsers.add_parser('from-files',
                                           help="generate integrity from files")
    command_parser.add_argument('--output',
                                dest='output_file',
                                help="output file name")
    command_parser.add_argument('file', help="files or directories to generate integrity for", nargs='+')

    command_parser = subparsers.add_parser('from-archive',
                                           help="generate integrity from the archive")
    command_parser.add_argument('--output',
                                dest='output_file',
                                help="output file name")
    command_parser.add_argument('--archive',
                                dest='archive', default="raw",
                                choices=["raw", "edited", "clean", "avgh", "avgd", "avgm"],
                                help="data file archive")
    command_parser.add_argument('--instrument',
                                dest='instruments', nargs='*',
                                help="instruments to match")
    command_parser.add_argument('station',
                                help="station code")
    command_parser.add_argument('time', help="time bounds to generate", nargs='+')

    command_parser = subparsers.add_parser('verify-files',
                                           help="verify integrity of direct files")
    command_parser.add_argument('--show-identical',
                                dest='show_identical', action='store_true',
                                help="generate output for identical files")
    command_parser.add_argument('--root',
                                dest='root_directory',
                                help="root directory containing files to verify")
    command_parser.add_argument('file', help="integrity files to verify", nargs='+')

    command_parser = subparsers.add_parser('verify-archive',
                                           help="verify integrity of the archive")
    command_parser.add_argument('--show-identical',
                                dest='show_identical', action='store_true',
                                help="generate output for identical files")
    command_parser.add_argument('file', help="integrity files to verify", nargs='+')

    args = parser.parse_args()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    async def get_archive() -> Connection:
        if args.archive_unix_socket:
            _LOGGER.debug("Opening archive Unix socket '%s'", args.archive_unix_socket)
            reader, writer = await asyncio.open_unix_connection(args.archive_unix_socket)
            connection = Connection(reader, writer, "integrity file")
        elif args.archive_tcp_server:
            _LOGGER.debug("Opening archive connection to %s port %d", args.archive_tcp_server, args.archive_tcp_port)
            reader, writer = await asyncio.open_connection(args.archive_tcp_server, args.archive_tcp_port)
            connection = Connection(reader, writer, "integrity file")
        else:
            _LOGGER.debug("Opening default archive connection")
            connection = await Connection.default_connection("integrity file")
        return connection

    async def run():
        if args.command == 'from-files':
            source: typing.List[Path] = list()
            for file in args.file:
                file = Path(file)
                if not file.exists():
                    parser.error(f"File '{file}' does not exist")
                    exit(1)
                source.append(file)
            if args.output_file:
                with open(args.output_file, "wt") as output_file:
                    await _generate_from_files(source, output_file)
            else:
                await _generate_from_files(source, sys.stdout)
        elif args.command == 'from-archive':
            station = args.station.lower()
            if station not in STATIONS:
                parser.error("Invalid station code")
                exit(1)
            start, end = parse_time_bounds_arguments(args.time)
            start = start.timestamp()
            end = end.timestamp()
            start_epoch_ms = int(floor(start * 1000))
            end_epoch_ms = int(ceil(end * 1000))

            instrument_match = set()

            def split_strip(v):
                result = []
                for add in re.split(r"[\s:;,]+", v):
                    add = add.strip()
                    if not add:
                        continue
                    result.append(add)
                return result

            if args.instruments:
                for add in args.instruments:
                    instrument_match.update(split_strip(add))

            async with await get_archive() as connection:
                await _generate_from_archive(
                    connection, station, args.archive, instrument_match, start_epoch_ms, end_epoch_ms,
                    args.output_file,
                )
        elif args.command == 'verify-files':
            if args.root_directory:
                root_directory = Path(args.root_directory)
                if not root_directory.is_dir():
                    parser.error(f"Root directory '{args.root_directory}' does not exist")
            else:
                root_directory = Path('.')
            if not await _verify_files(root_directory, args.file, args.show_identical):
                return 1
        elif args.command == 'verify-archive':
            async with await get_archive() as connection:
                if not await _verify_archive(connection, args.file, args.show_identical):
                    return 1

        return 0

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    rc = loop.run_until_complete(run())
    loop.close()
    sys.exit(rc)


if __name__ == '__main__':
    main()
