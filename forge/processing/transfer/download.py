#!/usr/bin/python3
import datetime
import typing
import argparse
import asyncio
import logging
import signal
import re
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from base64 import b64decode
from shutil import move as move_file
from forge.crypto import key_to_bytes
from forge.processing.transfer import CONFIGURATION
from forge.processing.transfer.completion import completion_directory
from forge.processing.transfer.storage.client import GetFiles
from forge.processing.transfer.storage.protocol import FileType


_LOGGER = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Forge data transfer downloader.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--socket',
                        dest='socket',
                        default=CONFIGURATION.get('PROCESSING.TRANSFER.SOCKET', '/run/forge-transfer-storage.socket'),
                        help="transfer server socket")
    parser.add_argument('--server-host',
                        dest='tcp_server',
                        help="transfer server host")
    parser.add_argument('--server-port',
                        dest='tcp_port',
                        type=int,
                        help="transfer server port")

    parser.add_argument('--station',
                        dest='station',
                        nargs='*',
                        help="filter by station code")
    parser.add_argument('--ignore-station',
                        dest='ignore_station',
                        nargs='*',
                        help="ignore by station code")
    parser.add_argument('--type',
                        dest='type',
                        choices=['data', 'backup', 'auxiliary'],
                        nargs='*',
                        help="filter by data type")
    parser.add_argument('--key',
                        dest='public_key',
                        nargs='*',
                        help="filter by public key")
    parser.add_argument('--filename',
                        dest='filename',
                        nargs='*',
                        help="filter by file name regular expression pattern")

    parser.add_argument('--directory',
                        dest='directory',
                        help="output directory instead of the current one")
    parser.add_argument('--temp-directory',
                        dest='temp_directory',
                        help="temporary directory instead of the system default")

    parser.add_argument('--command',
                        dest='command',
                        help="command to after file fetch")

    args = parser.parse_args()

    if args.tcp_server and not args.tcp_port:
        parser.error("Both a server host and port must be specified")
    elif not args.tcp_server and args.tcp_port:
        parser.error("Both a server host and port must be specified")
    elif not args.tcp_server and not args.socket:
        parser.error("Either a transfer server socket or host must be specified")

    return args


def main():
    args = parse_arguments()
    if args.debug:
        root_logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)-40s %(message)s')
        handler.setFormatter(formatter)
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)
        CONFIGURATION.DEBUG = True

    _LOGGER.info(f"Starting file downloader")

    keys: typing.Set[bytes] = set()
    if args.public_key:
        for key in args.public_key:
            key = b64decode(key)
            keys.add(key)

    file_types: typing.Set[FileType] = set()
    if args.type:
        for t in args.type:
            if t == 'data':
                file_types.add(FileType.DATA)
            elif t == 'backup':
                file_types.add(FileType.BACKUP)
            elif t == 'auxiliary':
                file_types.add(FileType.AUXILIARY)
            else:
                raise ValueError

    filenames: typing.List[re.Pattern] = list()
    if args.filename:
        for p in args.filename:
            filenames.append(re.compile(p))

    stations: typing.Set[str] = set()
    if args.station:
        for s in args.station:
            stations.add(s.lower())
    ignore_stations: typing.Set[str] = set()
    if args.ignore_station:
        for s in args.ignore_stations:
            ignore_stations.add(s.lower())

    output_directory: str = '.'
    if args.directory:
        output_directory = args.directory
    temp_directory: typing.Optional[Path] = None
    if args.temp_directory:
        temp_directory = Path(args.temp_directory)

    class _Client(GetFiles):
        class FetchFile(GetFiles.FetchFile):
            async def begin_fetch(self) -> typing.Optional[typing.BinaryIO]:
                if file_types and self.file_type not in file_types:
                    _LOGGER.debug(f"Rejected file type {self.station}/{self.filename}")
                    return None
                if stations and self.station not in stations:
                    _LOGGER.debug(f"Rejected station {self.station}/{self.filename}")
                    return None
                if ignore_stations and self.station in ignore_stations:
                    _LOGGER.debug(f"Rejected station {self.station}/{self.filename}")
                    return None
                if keys and key_to_bytes(self.key) not in keys:
                    _LOGGER.debug(f"Rejected key {self.station}/{self.filename}")
                    return None
                if filenames:
                    for check in filenames:
                        if check.search(self.filename):
                            break
                    else:
                        _LOGGER.debug(f"Rejected file {self.station}/{self.filename}")
                        return None

                _LOGGER.debug(f"Incoming file {self.filename}")
                return NamedTemporaryFile(prefix=self.filename,
                                          dir=str(temp_directory) if temp_directory else None,
                                          delete=False)

            async def complete(self, output: NamedTemporaryFile, ok: bool) -> None:
                if not ok:
                    try:
                        output.close()
                    except IOError:
                        pass
                    try:
                        os.unlink(output.name)
                    except IOError:
                        pass
                    pass

                output.close()

                file_directory = completion_directory(output_directory, self.key, self.station, self.file_type)
                output_path = str(Path(file_directory) / self.filename)
                move_file(output.name, output_path)

                if args.command:
                    process = await asyncio.create_subprocess_shell(
                        args.command,
                        stdin=asyncio.subprocess.DEVNULL
                    )
                    await process.communicate()

                _LOGGER.info(f"Received file {output_path}")

    async def run():
        if args.tcp_server and args.tcp_port:
            reader, writer = await asyncio.open_connection(args.tcp_server, args.tcp_port)
        else:
            reader, writer = await asyncio.open_unix_connection(args.socket)

        client = _Client(reader, writer)
        _LOGGER.info("Connected to transfer server")
        await client.run()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    get_run = loop.create_task(run())
    loop.add_signal_handler(signal.SIGINT, get_run.cancel)
    loop.add_signal_handler(signal.SIGTERM, get_run.cancel)
    try:
        loop.run_until_complete(get_run)
    except asyncio.CancelledError:
        pass
    loop.close()


if __name__ == '__main__':
    main()
