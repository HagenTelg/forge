#!/usr/bin/python3

import argparse
import asyncio
import logging
from base64 import b64encode, b64decode
from forge.crypto import PublicKey, key_to_bytes
from forge.dashboard import CONFIGURATION
from .interface import ControlInterface, Severity, is_valid_code, is_valid_station
from .display import display_entries_json, display_entries_text, display_access_json, display_access_text, sort_entries, sort_access


def add_entry_selection_arguments(parser):
    parser.add_argument('--entry',
                        dest='entry', type=int,
                        help="a specific entry ID number")
    parser.add_argument('--station',
                        dest='station',
                        help="match station code")
    parser.add_argument('--no-station',
                        dest='station', action='store_const', const='',
                        help="not applicable to a specific station")
    parser.add_argument('--code',
                        dest='entry_code',
                        help="match entry type code")
    parser.add_argument('--before',
                        dest='before', type=float,
                        help="last updated before X days ago")
    parser.add_argument('--after',
                        dest='after', type=float,
                        help="last updated after X days ago")
    parser.add_argument('--failed',
                        dest='failed', action='store_true',
                        help="reporting failed status")
    parser.add_argument('--ok',
                        dest='failed', action='store_false',
                        help="reporting nominal status")
    parser.set_defaults(failed=None)

    parser.add_argument('--notification',
                        dest='notification_severity',
                        choices=[v.value for v in Severity],
                        help="match notification severity")
    parser.add_argument('--notification-code',
                        dest='notification_code',
                        help="match notification code")

    parser.add_argument('--watchdog',
                        dest='watchdog_severity',
                        choices=[v.value for v in Severity],
                        help="match watchdog severity")
    parser.add_argument('--watchdog-code',
                        dest='watchdog_code',
                        help="match watchdog code")
    parser.add_argument('--watchdog-timeout',
                        dest='watchdog_timeout', type=float,
                        default=26.0,
                        help="watchdog timeout in hours")

    parser.add_argument('--event',
                        dest='event_severity',
                        choices=[v.value for v in Severity],
                        help="match event severity")
    parser.add_argument('--event-code',
                        dest='event_code',
                        help="match event code")

    parser.add_argument('--condition',
                        dest='condition_severity',
                        choices=[v.value for v in Severity],
                        help="match condition severity")
    parser.add_argument('--condition-code',
                        dest='condition_code',
                        help="match condition code")


def add_key_selection_arguments(parser):
    parser.add_argument('--key',
                        dest='public_key',
                        help="public key")
    parser.add_argument('--station',
                        dest='station',
                        help="match station code")
    parser.add_argument('--no-station',
                        dest='station', action='store_const', const='',
                        help="not applicable to a specific station")
    parser.add_argument('--code',
                        dest='entry_code',
                        help="match entry type code")
    parser.add_argument('--access',
                        dest='access',
                        help="a specific access number")


def add_bearer_selection_arguments(parser):
    parser.add_argument('--bearer',
                        dest='bearer_token',
                        help="bearer token")
    parser.add_argument('--station',
                        dest='station',
                        help="match station code")
    parser.add_argument('--no-station',
                        dest='station', action='store_const', const='',
                        help="not applicable to a specific station")
    parser.add_argument('--code',
                        dest='entry_code',
                        help="match entry type code")
    parser.add_argument('--access',
                        dest='access',
                        help="a specific access number")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Forge dashboard control.")

    parser.add_argument('--database',
                        dest='database_uri',
                        help="backend database URI")
    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")

    subparsers = parser.add_subparsers(dest='command')

    command_parser = subparsers.add_parser('list',
                                           help="list dashboard entries")
    add_entry_selection_arguments(command_parser)
    command_parser.add_argument('--json',
                                dest='json', action='store_true',
                                help="output entry list in JSON")
    command_parser.add_argument('--details',
                                dest='details', action='store_true',
                                help="output entry details")
    command_parser.add_argument('--sort',
                                dest='sort', default='station,code',
                                help="sort entries by field")
    command_parser.add_argument('--reverse',
                                dest='reverse', action='store_true',
                                help="reverse output order")

    command_parser = subparsers.add_parser('remove',
                                           help="remove entries")
    add_entry_selection_arguments(command_parser)
    command_parser.add_argument('--multiple',
                                dest='multiple', action='store_true',
                                help="required if a single entry is not selected")

    command_parser = subparsers.add_parser('purge-stale',
                                           help="remove stale information")
    add_entry_selection_arguments(command_parser)
    command_parser.add_argument('--multiple',
                                dest='multiple', action='store_true',
                                help="required if a single entry is not selected")
    command_parser.add_argument('--threshold',
                                dest='stale_threshold', type=float, default=32.0,
                                help="remove information set X days ago")
    command_parser.add_argument('--stale-watchdogs',
                                dest='stale_watchdogs', action='store_true',
                                help="remove stale watchdogs")
    command_parser.add_argument('--no-stale-watchdogs',
                                dest='stale_watchdogs', action='store_false',
                                help="do not remove stale watchdogs")
    command_parser.set_defaults(stale_watchdogs=True)
    command_parser.add_argument('--stale-events',
                                dest='stale_events', action='store_true',
                                help="remove stale events")
    command_parser.add_argument('--no-stale-events',
                                dest='stale_events', action='store_false',
                                help="do not remove stale events")
    command_parser.set_defaults(stale_events=True)
    command_parser.add_argument('--stale-conditions',
                                dest='stale_conditions', action='store_true',
                                help="remove stale conditions")
    command_parser.add_argument('--no-stale-conditions',
                                dest='stale_conditions', action='store_false',
                                help="do not remove stale conditions")
    command_parser.set_defaults(stale_conditions=True)

    command_parser = subparsers.add_parser('report-ok',
                                           help="report an entry as nominal")
    command_parser.add_argument('report_station',
                                help="station code")
    command_parser.add_argument('report_code',
                                help="entry type code")

    command_parser = subparsers.add_parser('report-failed',
                                           help="report an entry as failed")
    command_parser.add_argument('report_station',
                                help="station code")
    command_parser.add_argument('report_code',
                                help="entry type code")

    command_parser = subparsers.add_parser('key-list',
                                           help="list allowed public access keys")
    add_key_selection_arguments(command_parser)
    command_parser.add_argument('--json',
                                dest='json', action='store_true',
                                help="output key list in JSON")
    command_parser.add_argument('--sort',
                                dest='sort', default='station,code,public_key',
                                help="sort keys by field")
    command_parser.add_argument('--reverse',
                                dest='reverse', action='store_true',
                                help="reverse output order")

    command_parser = subparsers.add_parser('key-add',
                                           help="add an allowed access key")
    command_parser.add_argument('public_key',
                                help="public key")
    command_parser.add_argument('station',
                                help="station code")
    command_parser.add_argument('code',
                                help="entry type code")

    command_parser = subparsers.add_parser('key-remove',
                                           help="remove allowed public access keys")
    add_key_selection_arguments(command_parser)
    command_parser.add_argument('--multiple',
                                dest='multiple', action='store_true',
                                help="required if a single key is not selected")

    command_parser = subparsers.add_parser('key-generate',
                                           help="generate a private key")
    command_parser.add_argument('--simple',
                                dest='simple', action='store_true',
                                help="simple output mode")

    command_parser = subparsers.add_parser('bearer-list',
                                           help="list allowed bear access tokens")
    add_bearer_selection_arguments(command_parser)
    command_parser.add_argument('--json',
                                dest='json', action='store_true',
                                help="output bearer list in JSON")
    command_parser.add_argument('--sort',
                                dest='sort', default='station,code,bearer_token',
                                help="sort bearers by field")
    command_parser.add_argument('--reverse',
                                dest='reverse', action='store_true',
                                help="reverse output order")

    command_parser = subparsers.add_parser('bearer-add',
                                           help="add an allowed bearer access token")
    command_parser.add_argument('bearer_token',
                                help="bearer access token")
    command_parser.add_argument('station',
                                help="station code")
    command_parser.add_argument('code',
                                help="entry type code")

    command_parser = subparsers.add_parser('bearer-remove',
                                           help="remove allowed bearer access token")
    add_bearer_selection_arguments(command_parser)
    command_parser.add_argument('--multiple',
                                dest='multiple', action='store_true',
                                help="required if a single bearer token is not selected")

    command_parser = subparsers.add_parser('bearer-generate',
                                           help="generate a bearer token")
    command_parser.add_argument('--simple',
                                dest='simple', action='store_true',
                                help="simple output mode")

    command_parser = subparsers.add_parser('email-send',
                                           help="send emails to all subscribed users")
    add_entry_selection_arguments(command_parser)
    command_parser.add_argument('--multiple',
                                dest='multiple', action='store_true',
                                help="required if a single entry is not selected")
    command_parser.add_argument('--resend',
                                dest='resend', action='store_true',
                                help="resend the email without changing the current unsent information")
    command_parser.add_argument('--interval',
                                dest='interval', type=float, default=1,
                                help="minimum time in days to send for")
    command_parser.add_argument('--sort',
                                dest='sort', default='station,code',
                                help="sort entries by field")
    parser.add_argument('--access-database',
                        dest='access_database_uri',
                        help="access backend database URI")

    command_parser = subparsers.add_parser('email-reset',
                                           help="reset unsent email information")
    add_entry_selection_arguments(command_parser)
    command_parser.add_argument('--multiple',
                                dest='multiple', action='store_true',
                                help="required if a single entry is not selected")

    command_parser = subparsers.add_parser('email-show',
                                           help="output the contents of the email that would be sent")
    command_parser.add_argument('--html',
                                dest='html', action='store_true',
                                help="show the HTML email")
    command_parser.add_argument('--interval',
                                dest='interval', type=float, default=1,
                                help="minimum time in days to send for")
    command_parser.add_argument('email_station',
                                help="station code")
    command_parser.add_argument('email_code',
                                help="entry type code")

    args = parser.parse_args()

    if args.command == 'remove' and args.entry is None and not args.multiple:
        parser.error("--multiple required without --entry")
    if args.command == 'purge-stale' and args.entry is None and not args.multiple:
        parser.error("--multiple required without --entry")
    if args.command == 'report-ok' or args.command == 'report-failed':
        if args.report_station and args.report_station != '_' and not is_valid_station(args.report_station):
            parser.error("invalid station code")
        if not is_valid_code(args.report_code):
            parser.error("invalid entry type code")
    if args.command == 'key-remove' and args.access is None and not args.multiple:
        parser.error("--multiple required without --access")
    if args.command == 'bearer-remove' and args.access is None and not args.multiple:
        parser.error("--multiple required without --access")
    if args.command == 'email-send' and args.entry is None and not args.multiple:
        parser.error("--multiple required without --entry")
    if args.command == 'email-reset' and args.entry is None and not args.multiple:
        parser.error("--multiple required without --entry")
    if args.command == 'email_show':
        if args.email_station and args.email_station != '_' and not is_valid_station(args.email_station):
            parser.error("invalid station code")
        if not is_valid_code(args.email_code):
            parser.error("invalid entry type code")

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

    if args.command == 'key-generate':
        import secrets
        from forge.crypto import PrivateKey
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
    elif args.command == 'bearer-generate':
        import secrets
        bearer_token = b64encode(secrets.token_bytes(32)).decode('ascii')
        if args.simple:
            print(bearer_token)
        else:
            print("Please save the bearer token where required.")
            print("You will need to securely authorize the token.")
            print(f"Authorization: Bearer {bearer_token}")
        return

    database_uri = args.database_uri
    if database_uri is None:
        database_uri = CONFIGURATION.DASHBOARD.DATABASE

    async def run():
        interface = ControlInterface(database_uri)

        if args.command == 'list':
            entries = await interface.list_filtered(**vars(args))
            sort_keys = args.sort.split(',')
            if len(sort_keys) <= 0 or len(sort_keys[0]) <= 0:
                sort_keys = []
            sort_entries(sort_keys, entries)
            if args.reverse:
                entries.reverse()
            if args.json:
                display_entries_json(entries)
            else:
                display_entries_text(entries)
        elif args.command == 'remove':
            await interface.remove_entries(**vars(args))
        elif args.command == 'purge-stale':
            threshold = args.stale_threshold
            purge_watchdogs = args.stale_watchdogs
            purge_events = args.stale_events
            purge_conditions = args.stale_conditions
            await interface.purge_stale(threshold, purge_watchdogs, purge_events, purge_conditions, **vars(args))
        elif args.command == 'report-ok':
            station = args.report_station
            if station == '_':
                station = None
            if not station:
                station = None
            code = args.report_code
            await interface.report_status(station, code, False)
        elif args.command == 'report-failed':
            station = args.report_station
            if station == '_':
                station = None
            if not station:
                station = None
            code = args.report_code
            await interface.report_status(station, code, True)
        elif args.command == 'key-list':
            access = await interface.list_access_keys(**vars(args))
            sort_keys = args.sort.split(',')
            if len(sort_keys) <= 0 or len(sort_keys[0]) <= 0:
                sort_keys = []
            sort_access(sort_keys, access)
            if args.reverse:
                access.reverse()
            if args.json:
                display_access_json(access)
            else:
                display_access_text(access, 'public_key', "Public Key")
        elif args.command == 'key-add':
            public_key = PublicKey.from_public_bytes(b64decode(args.public_key))
            await interface.add_access_key(public_key, args.station, args.code)
        elif args.command == 'key-remove':
            await interface.remove_access_key(**vars(args))
        elif args.command == 'bearer-list':
            access = await interface.list_access_bearer(**vars(args))
            sort_keys = args.sort.split(',')
            if len(sort_keys) <= 0 or len(sort_keys[0]) <= 0:
                sort_keys = []
            sort_access(sort_keys, access)
            if args.reverse:
                access.reverse()
            if args.json:
                display_access_json(access)
            else:
                display_access_text(access, 'bearer_token', "Bearer Token")
        elif args.command == 'bearer-add':
            await interface.add_access_bearer(args.bearer_token, args.station, args.code)
        elif args.command == 'bearer-remove':
            await interface.remove_access_bearer(**vars(args))
        elif args.command == 'email-send':
            from .email import send_entry_emails
            await send_entry_emails(interface, args)
        elif args.command == 'email-reset':
            await interface.email_reset(**vars(args))
        elif args.command == 'email-show':
            from .email import output_email_contents
            await output_email_contents(interface, args)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.close()


if __name__ == '__main__':
    main()
