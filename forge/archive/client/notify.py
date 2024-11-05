import typing
import asyncio
import logging
import argparse
from math import floor, ceil
from forge.const import STATIONS
from forge.timeparse import parse_time_bounds_arguments
from forge.archive import CONFIGURATION
from forge.archive.client import data_notification_key
from forge.archive.client.connection import Connection, LockDenied, LockBackoff

_LOGGER = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Forge archive notification sender.")

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

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--archive',
                       dest='archive',
                       choices=["raw", "edited", "clean", "avgh", "avgd", "avgm"],
                       default="raw",
                       help="archive to send the notification for")
    group.add_argument('--key',
                       dest='key',
                       help="archive notification key")

    parser.add_argument('station', help="station code to notify")
    parser.add_argument('time', help="time bounds to notify", nargs='+')

    args = parser.parse_args()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()
    if args.tcp_server and not args.tcp_port:
        parser.error("Both a server host and port must be specified")

    station = args.station.lower()
    if station not in STATIONS:
        parser.error("Invalid station code")
    start, end = parse_time_bounds_arguments(args.time)
    start = start.timestamp()
    end = end.timestamp()
    start_epoch_ms = int(floor(start * 1000))
    end_epoch_ms = int(ceil(end * 1000))

    notify_key = args.key
    if not notify_key:
        notify_key = data_notification_key(station, args.archive)

    loop = asyncio.new_event_loop()

    async def run():
        if args.tcp_server and args.tcp_port:
            _LOGGER.debug(f"Connecting to archive TCP socket {args.tcp_server}:{args.tcp_port}")
            reader, writer = await asyncio.open_connection(args.tcp_server, int(args.tcp_port))
            connection = Connection(reader, writer, "manual notify")
        elif args.unix_socket:
            _LOGGER.debug(f"Connecting to archive Unix socket {args.unix_socket}")
            reader, writer = await asyncio.open_unix_connection(args.unix_socket)
            connection = Connection(reader, writer, "manual notify")
        else:
            connection = await Connection.default_connection("manual notify")

        await connection.startup()
        _LOGGER.debug("Sending notification for %s on %d,%d", notify_key, start_epoch_ms, end_epoch_ms)
        backoff = LockBackoff()
        while True:
            try:
                async with connection.transaction(True):
                    await connection.send_notification(notify_key, start_epoch_ms, end_epoch_ms)
                break
            except LockDenied as ld:
                _LOGGER.debug("Archive busy: %s", ld.status)
                await backoff()
        await connection.shutdown()

    loop.run_until_complete(run())
    loop.close()


if __name__ == '__main__':
    main()
