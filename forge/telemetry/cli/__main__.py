#!/usr/bin/python3

import argparse
import asyncio
import logging
from base64 import b64encode, b64decode
from forge.telemetry import CONFIGURATION, PublicKey, key_to_bytes
from forge.telemetry.storage import ControlInterface
from .display import sort_hosts, sort_access, display_json, display_hosts_text, display_details_text, display_login_text, display_access_text


def add_host_selection_arguments(parser):
    parser.add_argument('--key',
                        dest='public_key',
                        help="reporting key")
    parser.add_argument('--station',
                        dest='station',
                        help="reporting as a station")
    parser.add_argument('--host',
                        dest='host', type=int,
                        help="a specific host ID number")
    parser.add_argument('--before',
                        dest='before', type=float,
                        help="last seen before X days ago")
    parser.add_argument('--after',
                        dest='after', type=float,
                        help="last seen after X days ago")
    parser.add_argument('--public-address',
                        dest='remote_host',
                        help="reporting public address")


def add_access_selection_arguments(parser):
    parser.add_argument('--station',
                        dest='station',
                        help="allowed access to a station")
    parser.add_argument('--key',
                        dest='public_key',
                        help="public key")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Forge telemetry interface.")

    parser.add_argument('--database',
                        dest='database_uri',
                        help="backend database URI")
    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")

    subparsers = parser.add_subparsers(dest='command')

    command_parser = subparsers.add_parser('list',
                                           help="display host telemetry summary")
    add_host_selection_arguments(command_parser)
    command_parser.add_argument('--json',
                                dest='json', action='store_true',
                                help="output host list in JSON")
    command_parser.add_argument('--sort',
                                dest='sort', default='station,last_seen,public_key',
                                help="sort hosts by field")
    command_parser.add_argument('--reverse',
                                dest='reverse', action='store_true',
                                help="reverse output order")

    command_parser = subparsers.add_parser('details',
                                           help="display host telemetry details")
    add_host_selection_arguments(command_parser)
    command_parser.add_argument('--json',
                                dest='json', action='store_true',
                                help="output host list in JSON")
    command_parser.add_argument('--sort',
                                dest='sort', default='station,last_seen,public_key',
                                help="sort hosts by field")
    command_parser.add_argument('--reverse',
                                dest='reverse', action='store_true',
                                help="reverse output order")

    command_parser = subparsers.add_parser('address',
                                           help="get last public address of a host")
    add_host_selection_arguments(command_parser)
    command_parser.add_argument('--sort',
                                dest='sort', default='station,last_seen,public_key',
                                help="sort hosts by field")
    command_parser.add_argument('--reverse',
                                dest='reverse', action='store_true',
                                help="reverse output order")

    command_parser = subparsers.add_parser('login',
                                           help="get remote login information")
    add_host_selection_arguments(command_parser)
    command_parser.add_argument('--json',
                                dest='json', action='store_true',
                                help="output host list in JSON")
    command_parser.add_argument('--sort',
                                dest='sort', default='station,last_seen,public_key',
                                help="sort hosts by field")
    command_parser.add_argument('--reverse',
                                dest='reverse', action='store_true',
                                help="reverse output order")

    command_parser = subparsers.add_parser('access-list',
                                           help="list access rights")
    add_access_selection_arguments(command_parser)
    command_parser.add_argument('--json',
                                dest='json', action='store_true',
                                help="output host list in JSON")

    command_parser = subparsers.add_parser('access-grant',
                                           help="grant access")
    command_parser.add_argument('grant_key', nargs=1,
                                help="public key")
    command_parser.add_argument('grant_station', nargs='*',
                                help="station code")

    command_parser = subparsers.add_parser('access-revoke',
                                           help="revoke access")
    add_access_selection_arguments(command_parser)
    command_parser.add_argument('--multiple',
                                dest='multiple', action='store_true',
                                help="required if a single station is not selected")

    command_parser = subparsers.add_parser('purge',
                                           help="purge telemetry")
    add_host_selection_arguments(command_parser)
    command_parser.add_argument('--multiple',
                                dest='multiple', action='store_true',
                                help="required if a single key is not selected")

    command_parser = subparsers.add_parser('generate-key',
                                           help="generate a private key")
    command_parser.add_argument('--simple',
                                dest='simple', action='store_true',
                                help="simple output mode")

    args = parser.parse_args()
    if args.command == 'access-revoke' and args.public_key is None and not args.multiple:
        parser.error("--multiple required when revoking access without a key")
    elif args.command == 'purge' and args.public_key is None and args.host is None and not args.multiple:
        parser.error("--multiple required when purging hosts without a key or host")
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

    if args.command == 'generate-key':
        import secrets
        from forge.telemetry import PrivateKey
        private_key = PrivateKey.from_private_bytes(secrets.token_bytes(32))
        public_key = b64encode(key_to_bytes(private_key.public_key())).decode('ascii')
        private_key = b64encode(key_to_bytes(private_key)).decode('ascii')
        if args.simple:
            print(private_key)
            print(public_key)
        else:
            print("Please save the private key where required")
            print("***DO NOT EMAIL OR PUBLISH THE PRIVATE KEY***")
            print(f"Private Key: {private_key}")
            print(f"Public Key : {public_key}")
        return

    database_uri = args.database_uri
    if database_uri is None:
        database_uri = CONFIGURATION.TELEMETRY.DATABASE

    async def run():
        interface = ControlInterface(database_uri)

        def apply_sort(hosts):
            sort_keys = args.sort.split(',')
            if len(sort_keys) <= 0 or len(sort_keys[0]) <= 0:
                sort_keys = []
            sort_hosts(sort_keys, hosts)
            if args.reverse:
                hosts.reverse()

        if args.command == 'list':
            hosts = await interface.list_hosts(**vars(args))
            apply_sort(hosts)
            if args.json:
                display_json(hosts)
            else:
                display_hosts_text(hosts)
        elif args.command == 'details':
            hosts = await interface.host_details(**vars(args))
            apply_sort(hosts)
            if args.json:
                display_json(hosts)
            else:
                display_details_text(hosts)
        elif args.command == 'address':
            hosts = await interface.list_hosts(**vars(args))
            apply_sort(hosts)
            for h in reversed(hosts):
                remote_host = h.get('remote_host')
                if not remote_host:
                    continue
                print(remote_host)
                break
        elif args.command == 'login':
            hosts = await interface.login_info(**vars(args))
            apply_sort(hosts)
            if args.json:
                display_json(hosts)
            else:
                display_login_text(hosts)
        elif args.command == 'purge':
            await interface.purge_hosts(**vars(args))
        elif args.command == 'access-list':
            access = await interface.list_access(**vars(args))
            sort_access(access)
            if args.json:
                display_json(access)
            else:
                display_access_text(access)
        elif args.command == 'access-grant':
            public_key = PublicKey.from_public_bytes(b64decode(args.grant_key[0]))
            stations = args.grant_station
            if len(stations) == 0:
                stations = ['*']
            elif len(stations) == 1:
                stations = stations[0].split(',')
            await interface.grant_station_access(public_key, stations)
        elif args.command == 'access-revoke':
            await interface.access_revoke(**vars(args))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    loop.close()


if __name__ == '__main__':
    main()