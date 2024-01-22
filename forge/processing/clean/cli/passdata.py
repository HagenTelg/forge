import typing
import logging
import asyncio
import argparse
import getpass
import os
import shlex
import sys
import time
from math import floor, ceil
from tempfile import NamedTemporaryFile
from forge.const import STATIONS
from forge.timeparse import parse_time_bounds_arguments
from forge.logicaltime import containing_year_range, start_of_year
from forge.archive.client import passed_lock_key, passed_file_name, passed_notification_key
from forge.archive.client.connection import Connection, LockDenied, LockBackoff
from forge.processing.clean.passing import apply_pass

_LOGGER = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Pass data into the clean archive")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--profile',
                        dest='profile', default='aerosol',
                        help="profile of data")
    parser.add_argument('--comment',
                        dest='comment',
                        help="comment attached to the pass operation")
    parser.add_argument('station', help="station code")
    parser.add_argument('time', help="time bounds to pass", nargs='+')

    args = parser.parse_args()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    station = args.station.lower()
    if station not in STATIONS:
        parser.error("Invalid station code")
    start, end = parse_time_bounds_arguments(args.time)
    start = start.timestamp()
    end = end.timestamp()

    profile = args.profile
    profile = profile.strip().lower()
    if not profile:
        parser.error("Invalid profile")
    comment = args.comment
    if not comment:
        comment = ""

    auxiliary = {
        "type": "command_line",
        "command": " ".join(map(shlex.quote, sys.argv)),
        "user": getpass.getuser(),
    }
    try:
        auxiliary["uid"] = os.getuid()
    except AttributeError:
        pass
    try:
        auxiliary["gid"] = os.getgid()
    except AttributeError:
        pass
    try:
        auxiliary["pid"] = os.getpid()
    except AttributeError:
        pass
    try:
        import socket
        auxiliary["hostname"] = socket.gethostname()
    except AttributeError:
        pass

    loop = asyncio.new_event_loop()

    async def run():
        async with await Connection.default_connection("pass data command") as connection:
            backoff = LockBackoff()
            while True:
                try:
                    async with connection.transaction(True):
                        await apply_pass(
                            connection, station, profile,
                            start, end,
                            comment, auxiliary,
                        )
                    break
                except LockDenied as ld:
                    _LOGGER.debug("Archive busy: %s", ld.status)
                    if sys.stdout.isatty():
                        if not backoff.has_failed:
                            sys.stdout.write("\n")
                        sys.stdout.write(f"\x1B[2K\rBusy: {ld.status}")
                    await backoff()
                    continue

    loop.run_until_complete(run())
    loop.close()


if __name__ == '__main__':
    main()