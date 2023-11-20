#!/usr/bin/env python3
import typing
import argparse
import asyncio
import logging
import struct
import time
from pathlib import Path
from forge.const import STATIONS
from forge.crypto import PublicKey, key_to_bytes
from forge.processing.transfer import CONFIGURATION
from forge.processing.transfer.ingest.file import file_error


_LOGGER = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Forge acquisition data ingest notification.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--socket',
                        dest='socket',
                        default=CONFIGURATION.get('PROCESSING.TRANSFER.DATA.SOCKET', '/run/forge-transfer-ingest.socket'),
                        help="ingest controller server socket")
    parser.add_argument('--server-host',
                        dest='tcp_server',
                        help="ingest controller server host")
    parser.add_argument('--server-port',
                        dest='tcp_port',
                        type=int,
                        help="ingest controller server port")

    parser.add_argument('--station',
                        dest='station',
                        help="override destination station")
    parser.add_argument('--key',
                        dest='public_key',
                        help="set the public key")

    parser.add_argument('file',
                        help="files to process",
                        nargs='+')

    args = parser.parse_args()

    if args.tcp_server and not args.tcp_port:
        parser.error("Both a server host and port must be specified")
    elif not args.tcp_server and args.tcp_port:
        parser.error("Both a server host and port must be specified")
    elif not args.tcp_server and not args.socket:
        parser.error("Either a transfer server socket or host must be specified")

    return args


async def send_file_notification(reader: asyncio.StreamReader,  writer: asyncio.StreamWriter, file_name: str,
                                 station: typing.Optional[str] = None, key: typing.Optional[PublicKey] = None) -> bool:
    raw_name = file_name.encode('utf-8')
    writer.write(struct.pack('<BI', 0, len(raw_name)))
    writer.write(raw_name)

    if station:
        raw_station = station.lower().encode('utf-8')
        writer.write(struct.pack('<I', len(raw_station)))
        writer.write(raw_station)
    else:
        writer.write(struct.pack('<I', 0))

    if key:
        writer.write(struct.pack('<B', 1))
        writer.write(key_to_bytes(key))
    else:
        writer.write(struct.pack('<B', 0))

    await writer.drain()
    response = struct.unpack('<B', await reader.readexactly(1))[0]
    return response == 0


def main():
    args = parse_arguments()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()
        CONFIGURATION.DEBUG = True

    _LOGGER.info(f"Starting file notification")

    for file_name in args.file:
        check = Path(file_name)
        if not check.exists() or not check.is_file():
            _LOGGER.error(f"File {file_name} does not exist")
            exit(1)

    if args.station:
        if args.station.lower() not in STATIONS:
            _LOGGER.error(f"Invalid station {args.station.upper()}")
            exit(1)

    key = None
    if args.public_key:
        from base64 import b64decode
        key = PublicKey.from_public_bytes(b64decode(args.public_key))

    loop = asyncio.new_event_loop()

    any_errors = False

    async def run():
        nonlocal any_errors
        if args.tcp_server and args.tcp_port:
            _LOGGER.debug(f"Connecting to archive TCP socket {args.tcp_server}:{args.tcp_port}")
            reader, writer = await asyncio.open_connection(args.tcp_server, int(args.tcp_port))
        else:
            _LOGGER.debug(f"Connecting to archive Unix socket {args.socket}")
            reader, writer = await asyncio.open_unix_connection(args.socket)

        for file_name in args.file:
            file_begin = time.monotonic()
            if not await send_file_notification(reader, writer, file_name, args.station, key):
                _LOGGER.error(f"Failed to process {file_name}")
                any_errors = True
                if args.station:
                    await file_error(Path(file_name), args.station.lower(), time.monotonic() - file_begin)

        writer.close()

    loop.run_until_complete(run())
    loop.close()

    if any_errors:
        exit(1)


if __name__ == '__main__':
    main()
