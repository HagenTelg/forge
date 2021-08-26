#!/usr/bin/python3

import argparse
import asyncio
import logging
from os.path import exists as file_exists
from forge.vis import CONFIGURATION
from forge.vis.access.database import ControlInterface
from .display import display_users_json, display_users_text, sort_users


def add_user_selection_arguments(parser):
    parser.add_argument('--user',
                        dest='user', type=int,
                        help="a specific user ID number")
    parser.add_argument('--name',
                        dest='name',
                        help="match real names")
    parser.add_argument('--email',
                        dest='email',
                        help="match email addresses")
    parser.add_argument('--initials',
                        dest='initials',
                        help="match initials")
    parser.add_argument('--before',
                        dest='before', type=float,
                        help="last seen before login X days ago")
    parser.add_argument('--after',
                        dest='after', type=float,
                        help="last seen after login X days ago")
    parser.add_argument('--never-seen',
                        dest='never', action='store_true',
                        help="never seen logged in")
    parser.add_argument('--station',
                        dest='station',
                        help="allowed access to a station")
    parser.add_argument('--mode',
                        dest='mode',
                        help="allowed mode access")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Forge visualization user access control.")

    parser.add_argument('--database',
                        dest='database_uri',
                        help="backend database URI")
    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")

    subparsers = parser.add_subparsers(dest='command')

    command_parser = subparsers.add_parser('list',
                                           help="list users")
    add_user_selection_arguments(command_parser)
    command_parser.add_argument('--json',
                                dest='json', action='store_true',
                                help="output user list in JSON")
    command_parser.add_argument('--sort',
                                dest='sort', default='name,email,station,mode',
                                help="sort users by field")
    command_parser.add_argument('--reverse',
                                dest='reverse', action='store_true',
                                help="reverse output order")

    command_parser = subparsers.add_parser('grant',
                                           help="grant access")
    add_user_selection_arguments(command_parser)
    command_parser.add_argument('--no-confirm',
                                dest='no_confirm', action='store_true',
                                help="disable confirmation email")
    command_parser.add_argument('--read-only',
                                dest='read_only', action='store_true',
                                help="grant read only access")
    command_parser.add_argument('--multiple',
                                dest='multiple', action='store_true',
                                help="required if a single user is not selected")
    command_parser.add_argument('grant_station', nargs=1,
                                help="station code")
    command_parser.add_argument('grant', nargs='*',
                                help="access mode")

    command_parser = subparsers.add_parser('revoke',
                                           help="revoke access")
    add_user_selection_arguments(command_parser)
    command_parser.add_argument('--multiple',
                                dest='multiple', action='store_true',
                                help="required if a single user is not selected")

    command_parser = subparsers.add_parser('logout',
                                           help="logout users")
    add_user_selection_arguments(command_parser)
    command_parser.add_argument('--multiple',
                                dest='multiple', action='store_true',
                                help="required if a single user is not selected")

    command_parser = subparsers.add_parser('delete-user',
                                           help="delete users")
    add_user_selection_arguments(command_parser)
    command_parser.add_argument('--multiple',
                                dest='multiple', action='store_true',
                                help="required if a single user is not selected")

    command_parser = subparsers.add_parser('add-user',
                                           help="add a user")
    command_parser.add_argument('email', nargs=1,
                                help="user email")
    command_parser.add_argument('password', nargs='?',
                                help="password or a file to read")
    command_parser.add_argument('--name',
                                dest='name',
                                help="real name")
    command_parser.add_argument('--initials',
                                dest='initials',
                                help="user initials")

    command_parser = subparsers.add_parser('modify-user',
                                           help="modify users")
    add_user_selection_arguments(command_parser)
    command_parser.add_argument('--multiple',
                                dest='multiple', action='store_true',
                                help="required if a single user is not selected")
    command_parser.add_argument('--set-email',
                                dest='set_email',
                                help="set email address")
    command_parser.add_argument('--set-name',
                                dest='set_name',
                                help="set real name")
    command_parser.add_argument('--set-initials',
                                dest='set_initials',
                                help="set initials")
    command_parser.add_argument('--set-password',
                                dest='set_password',
                                help="set password or a file to read")

    args = parser.parse_args()

    if args.command == 'grant' and args.user is None and not args.multiple:
        parser.error("--multiple required when granting access without --user")
    if args.command == 'revoke' and args.user is None and not args.multiple:
        parser.error("--multiple required when revoking access without --user")
    if args.command == 'logout' and args.user is None and not args.multiple:
        parser.error("--multiple required when logging out without --user")
    if args.command == 'delete-user' and args.user is None and not args.multiple:
        parser.error("--multiple required when deleting without --user")
    if args.command == 'modify-user' and args.user is None and not args.multiple:
        parser.error("--multiple required when modifying without --user")

    return args


def get_password(password: str) -> str:
    if password and file_exists(password):
        with open(password, 'r') as f:
            password = f.read().strip()
    return password


def main():
    args = parse_arguments()
    if args.debug:
        root_logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)-31s %(message)s')
        handler.setFormatter(formatter)
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)

    database_uri = args.database_uri
    if database_uri is None:
        database_uri = CONFIGURATION.AUTHENTICATION.DATABASE

    async def run():
        interface = ControlInterface(database_uri)

        if args.command == 'list':
            users = await interface.list_users(**vars(args))
            sort_keys = args.sort.split(',')
            if len(sort_keys) > 0 and len(sort_keys[0]) > 0:
                sort_users(sort_keys, users)
            if args.reverse:
                users.reverse()
            if args.json:
                display_users_json(users)
            else:
                display_users_text(users)
        elif args.command == 'grant':
            grant_modes = args.grant
            if len(grant_modes) == 0:
                grant_modes = ['*']
            stations = args.grant_station
            if len(stations) == 1:
                stations = stations[0].split(',')
            await interface.grant_access(stations, grant_modes,
                                         immediate=args.no_confirm, write=(not args.read_only),
                                         **vars(args))
        elif args.command == 'revoke':
            await interface.revoke_access(**vars(args))
        elif args.command == 'logout':
            await interface.logout_user(**vars(args))
        elif args.command == 'delete-user':
            await interface.delete_user(**vars(args))
        elif args.command == 'add-user':
            email = args.email[0].strip()
            password = get_password(args.password)
            await interface.add_user(email, password, args.name, args.initials)
        elif args.command == 'modify-user':
            args.set_password = get_password(args.set_password)
            await interface.modify_user(**vars(args))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    loop.close()


if __name__ == '__main__':
    main()
