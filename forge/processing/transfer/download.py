#!/usr/bin/env python3
import datetime
import typing
import argparse
import asyncio
import logging
import signal
import shutil
import re
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from base64 import b64decode
from forge.crypto import key_to_bytes
from forge.processing.transfer import CONFIGURATION
from forge.processing.transfer.completion import completion_directory, completion_command
from forge.processing.transfer.storage.client import GetFiles
from forge.processing.transfer.storage.protocol import FileType


_LOGGER = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Forge data transfer downloader.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--systemd',
                        dest='systemd', action='store_true',
                        help="enable systemd integration")

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
                        action='append',
                        help="filter by station code")
    parser.add_argument('--ignore-station',
                        dest='ignore_station',
                        action='append',
                        help="ignore by station code")
    parser.add_argument('--type',
                        dest='type',
                        choices=['data', 'backup', 'auxiliary'],
                        action='append',
                        help="filter by data type")
    parser.add_argument('--key',
                        dest='public_key',
                        action='append',
                        help="filter by public key")
    parser.add_argument('--filename',
                        dest='filename',
                        action='append',
                        help="filter by file name regular expression pattern")

    parser.add_argument('--directory',
                        dest='directory',
                        help="output directory instead of the current one")
    parser.add_argument('--temp-directory',
                        dest='temp_directory',
                        help="temporary directory instead of the system default")

    parser.add_argument('--command',
                        dest='command',
                        help="command to run after file fetch")

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
        from forge.log import set_debug_logger
        set_debug_logger()
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

    filenames: typing.List["re.Pattern"] = list()
    if args.filename:
        for p in args.filename:
            filenames.append(re.compile(p))

    stations: typing.Set[str] = set()
    if args.station:
        for s in args.station:
            for add in s.split(','):
                stations.add(add.lower())
    ignore_stations: typing.Set[str] = set()
    if args.ignore_station:
        for s in args.ignore_station:
            for add in s.split(','):
                ignore_stations.add(add.lower())

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
                    return

                # NamedTemporaryFile hard codes the mode to 0o600, so manually apply the umask now
                try:
                    umask = os.umask(0o666) | 0o111
                    os.umask(umask)
                except NotImplementedError:
                    umask = 0o033
                file_mode = 0o666 & ~umask
                try:
                    os.chmod(output.fileno(), file_mode)
                except NotImplementedError:
                    os.chmod(output.name, file_mode)

                output.close()

                file_directory = completion_directory(output_directory, self.key, self.station, self.file_type)
                file_directory = Path(file_directory)
                file_directory.mkdir(parents=True, exist_ok=True)
                output_path = str(file_directory / self.filename)
                await asyncio.get_event_loop().run_in_executor(None, shutil.move, output.name, output_path)

                if args.command:
                    process = await asyncio.create_subprocess_shell(
                        completion_command(args.command, self.key, self.station, self.file_type),
                        stdin=asyncio.subprocess.DEVNULL
                    )
                    await process.communicate()

                _LOGGER.info(f"Received file {output_path}")

    reader: typing.Optional[asyncio.StreamReader] = None
    writer: typing.Optional[asyncio.StreamWriter] = None

    loop = asyncio.new_event_loop()

    async def start():
        nonlocal reader
        nonlocal writer

        if args.tcp_server and args.tcp_port:
            _LOGGER.debug(f"Connecting to transfer TCP socket {args.tcp_server}:{args.tcp_port}")
            reader, writer = await asyncio.open_connection(args.tcp_server, args.tcp_port)
        else:
            _LOGGER.debug(f"Connecting to transfer Unix socket {args.socket}")
            reader, writer = await asyncio.open_unix_connection(args.socket)

    loop.run_until_complete(start())

    async def run():
        client = _Client(reader, writer)
        _LOGGER.info("Connected to transfer server")
        await client.run()

    heartbeat: typing.Optional[asyncio.Task] = None
    if args.systemd:
        import systemd.daemon
        systemd.daemon.notify("READY=1")

        _LOGGER.debug("Starting systemd heartbeat")

        async def send_heartbeat() -> typing.NoReturn:
            while True:
                await asyncio.sleep(10)
                systemd.daemon.notify("WATCHDOG=1")

        heartbeat = loop.create_task(send_heartbeat())

    asyncio.set_event_loop(loop)
    get_run = loop.create_task(run())
    loop.add_signal_handler(signal.SIGINT, get_run.cancel)
    loop.add_signal_handler(signal.SIGTERM, get_run.cancel)
    try:
        loop.run_until_complete(get_run)
    except asyncio.CancelledError:
        pass

    if heartbeat:
        _LOGGER.debug("Shutting down heartbeat")
        t = heartbeat
        heartbeat = None
        try:
            t.cancel()
        except:
            pass
        try:
            loop.run_until_complete(t)
        except:
            pass

    try:
        writer.close()
    except:
        pass

    loop.close()


if __name__ == '__main__':
    main()
