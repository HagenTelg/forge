#!/usr/bin/python3

import argparse
import asyncio
import logging
from base64 import b64decode
from forge.crypto import PublicKey
from forge.telemetry import CONFIGURATION
from forge.telemetry.storage import ControlInterface
from .display import  sort_access, display_json, display_access_text
from .interface import ControlInterface


def add_access_selection_arguments(parser):
    parser.add_argument('--access',
                        dest='access', type=int,
                        help="a specific access ID number")
    parser.add_argument('--station',
                        dest='station',
                        help="allowed access to a station")
    parser.add_argument('--key',
                        dest='public_key',
                        help="public key")

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--acquisition',
                       dest='acquisition', action='store_true',
                       help="realtime acquisition data allowed")
    group.add_argument('--no-acquisition',
                       dest='acquisition', action='store_false',
                       help="realtime acquisition data not allowed")
    parser.set_defaults(acquisition=None)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Forge processing control interface.")

    parser.add_argument('--database',
                        dest='database_uri',
                        help="backend database URI")
    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")

    subparsers = parser.add_subparsers(dest='command')

    command_parser = subparsers.add_parser('access-list',
                                           help="display access summary")
    add_access_selection_arguments(command_parser)
    command_parser.add_argument('--json',
                                dest='json', action='store_true',
                                help="output access list in JSON")

    command_parser = subparsers.add_parser('access-grant',
                                           help="grant access")
    command_parser.add_argument('--no-revoke',
                                dest='no_revoke', action='store_true',
                                help="do not revoke existing station keys")
    group = command_parser.add_mutually_exclusive_group()
    group.add_argument('--acquisition',
                       dest='acquisition', action='store_true',
                       help="grant access for realtime acquisition data")
    group.add_argument('--no-acquisition',
                       dest='acquisition', action='store_false',
                       help="revoke or do not grant access for realtime acquisition data")
    command_parser.set_defaults(acquisition=True)
    command_parser.add_argument('grant_key', nargs=1,
                                help="public key")
    command_parser.add_argument('grant_station', nargs=1,
                                help="station code")

    command_parser = subparsers.add_parser('access-revoke',
                                           help="revoke access")
    add_access_selection_arguments(command_parser)
    command_parser.add_argument('--multiple',
                                dest='multiple', action='store_true',
                                help="required if a access entry is not selected")

    args = parser.parse_args()
    if args.command == 'access-revoke' and args.access is None and not args.multiple:
        parser.error("--multiple required when revoking access without selecting a single ID")
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

    database_uri = args.database_uri
    if database_uri is None:
        database_uri = CONFIGURATION.PROCESSING.CONTROL.DATABASE

    async def run():
        interface = ControlInterface(database_uri)

        if args.command == 'access-list':
            access = await interface.list_access(**vars(args))
            sort_access(access)
            if args.json:
                display_json(access)
            else:
                display_access_text(access)
        elif args.command == 'access-grant':
            public_key = PublicKey.from_public_bytes(b64decode(args.grant_key[0]))
            station = args.grant_station[0]
            await interface.set_access(public_key, station,
                                       revoke_existing=(not args.no_revoke),
                                       acquisition=args.acquisition)
        elif args.command == 'access-revoke':
            await interface.revoke_access(**vars(args))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.close()


if __name__ == '__main__':
    main()