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
from forge.processing.transfer.ingest.notify import send_file_notification


_LOGGER = logging.getLogger(__name__)
_VALID_FILE_NAME = re.compile(
    r'[A-Za-z][0-9A-Za-z_]{0,31}-[A-Z][A-Z0-9]*_'
    r'a[0-9]{8}T[0-9]{6}Z_'
    r'u[A-Z0-9]{16}\.nc',
    flags=re.IGNORECASE
)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Forge acquisition data transfer receiver.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--systemd',
                        dest='systemd', action='store_true',
                        help="enable systemd integration")

    parser.add_argument('--socket',
                        dest='transfer_socket',
                        default=CONFIGURATION.get('PROCESSING.TRANSFER.SOCKET', '/run/forge-transfer-storage.socket'),
                        help="transfer server socket")
    parser.add_argument('--server-host',
                        dest='transfer_tcp_server',
                        help="transfer server host")
    parser.add_argument('--server-port',
                        dest='transfer_tcp_port',
                        type=int,
                        help="transfer server port")

    parser.add_argument('--ingest-socket',
                        dest='ingest_socket',
                        default=CONFIGURATION.get('PROCESSING.TRANSFER.DATA.SOCKET',
                                                  '/run/forge-transfer-ingest.socket'),
                        help="ingest controller server socket")
    parser.add_argument('--ingest-server-host',
                        dest='ingest_tcp_server',
                        help="ingest controller server host")
    parser.add_argument('--ingest-server-port',
                        dest='ingest_tcp_port',
                        type=int,
                        help="ingest controller server port")

    parser.add_argument('--station',
                        dest='station',
                        action='append',
                        help="filter by station code")
    parser.add_argument('--ignore-station',
                        dest='ignore_station',
                        action='append',
                        help="ignore by station code")
    parser.add_argument('--key',
                        dest='public_key',
                        action='append',
                        help="filter by public key")

    args = parser.parse_args()

    if args.transfer_tcp_server and not args.transfer_tcp_port:
        parser.error("Both a server host and port must be specified")
    elif not args.transfer_tcp_server and args.transfer_tcp_port:
        parser.error("Both a server host and port must be specified")
    elif not args.transfer_tcp_server and not args.transfer_socket:
        parser.error("Either a transfer server socket or host must be specified")

    if args.ingest_tcp_server and not args.ingest_tcp_port:
        parser.error("Both a server host and port must be specified")
    elif not args.ingest_tcp_server and args.ingest_tcp_port:
        parser.error("Both a server host and port must be specified")
    elif not args.ingest_tcp_server and not args.ingest_socket:
        parser.error("Either a ingest server socket or host must be specified")

    return args


def main():
    args = parse_arguments()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()
        CONFIGURATION.DEBUG = True

    _LOGGER.info(f"Starting data receiver")

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

    keys: typing.Set[bytes] = set()
    if args.public_key:
        for key in args.public_key:
            key = b64decode(key)
            keys.add(key)

    incoming_directory = CONFIGURATION.get('PROCESSING.TRANSFER.DATA.INCOMING')

    ingest_reader: typing.Optional[asyncio.StreamReader] = None
    ingest_writer: typing.Optional[asyncio.StreamWriter] = None

    class _Client(GetFiles):
        class FetchFile(GetFiles.FetchFile):
            async def begin_fetch(self) -> typing.Optional[typing.BinaryIO]:
                if self.file_type != FileType.DATA:
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
                if not _VALID_FILE_NAME.fullmatch(self.filename):
                    _LOGGER.debug(f"Rejected file {self.station}/{self.filename}")
                    return None

                if incoming_directory:
                    temp_directory = completion_directory(incoming_directory, self.key, self.station, FileType.DATA)
                    Path(temp_directory).mkdir(parents=True, exist_ok=True)
                    _LOGGER.debug(f"Incoming file {self.filename}")
                    return NamedTemporaryFile(prefix='.incoming_' + self.filename, dir=temp_directory, delete=False)
                else:
                    _LOGGER.debug(f"Incoming temporary file {self.filename}")
                    return NamedTemporaryFile(prefix=self.filename, delete=False)

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
                    umask = os.umask(0o666)
                    os.umask(umask)
                    umask |= 0o111
                except NotImplementedError:
                    umask = 0o033
                file_mode = 0o666 & ~umask
                try:
                    os.chmod(output.fileno(), file_mode)
                except NotImplementedError:
                    os.chmod(output.name, file_mode)

                output.close()

                if incoming_directory:
                    file_directory = completion_directory(incoming_directory, self.key, self.station, self.file_type)
                    file_directory = Path(file_directory)
                    file_directory.mkdir(parents=True, exist_ok=True)
                    output_path = str(file_directory / self.filename)
                    await asyncio.get_event_loop().run_in_executor(None, shutil.move, output.name, output_path)
                else:
                    output_path = output.name

                if not await send_file_notification(ingest_reader, ingest_writer, output_path, self.station, self.key):
                    _LOGGER.error(f"Ingest notification rejected for {output_path}")

                    error_destination = CONFIGURATION.get('PROCESSING.TRANSFER.DATA.ERROR')
                    if error_destination is not None:
                        if not error_destination:
                            try:
                                os.unlink(output_path)
                            except IOError:
                                pass
                        else:
                            error_destination = completion_directory(error_destination, self.key, self.station,
                                                                     self.file_type)
                            try:
                                error_destination = Path(error_destination)
                                error_destination.mkdir(parents=True, exist_ok=True)
                                error_destination = str(error_destination / self.filename)
                                await asyncio.get_event_loop().run_in_executor(None, shutil.move,
                                                                               output_path, error_destination)
                            except:
                                _LOGGER.warning(f"Error completion failed for {output_path}", exc_info=True)
                else:
                    _LOGGER.info(f"Received file {output_path}")

    transfer_reader: typing.Optional[asyncio.StreamReader] = None
    transfer_writer: typing.Optional[asyncio.StreamWriter] = None

    loop = asyncio.new_event_loop()

    async def start():
        nonlocal transfer_reader
        nonlocal transfer_writer
        nonlocal ingest_reader
        nonlocal ingest_writer

        if args.transfer_tcp_server and args.transfer_tcp_port:
            _LOGGER.debug(f"Connecting to transfer TCP socket {args.transfer_tcp_server}:{args.transfer_tcp_port}")
            transfer_reader, transfer_writer = await asyncio.open_connection(args.transfer_tcp_server, args.transfer_tcp_port)
        else:
            _LOGGER.debug(f"Connecting to transfer Unix socket {args.transfer_socket}")
            transfer_reader, transfer_writer = await asyncio.open_unix_connection(args.transfer_socket)
            
        if args.ingest_tcp_server and args.ingest_tcp_port:
            _LOGGER.debug(f"Connecting to ingest TCP socket {args.ingest_tcp_server}:{args.ingest_tcp_port}")
            ingest_reader, ingest_writer = await asyncio.open_connection(args.ingest_tcp_server, args.ingest_tcp_port)
        else:
            _LOGGER.debug(f"Connecting to ingest Unix socket {args.ingest_socket}")
            ingest_reader, ingest_writer = await asyncio.open_unix_connection(args.ingest_socket)

    loop.run_until_complete(start())

    async def run():
        client = _Client(transfer_reader, transfer_writer)
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
        ingest_writer.close()
    except:
        pass
    try:
        transfer_writer.close()
    except:
        pass

    loop.close()


if __name__ == '__main__':
    main()
