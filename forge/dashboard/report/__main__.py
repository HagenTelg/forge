#!/usr/bin/python3

import typing
import argparse
import asyncio
import logging
from forge.dashboard.report.send import report_ok, report_failed, dashboard_report


def add_reporting_arguments(parser):
    parser.add_argument('--url',
                        dest='url',
                        help="reporting URL")
    parser.add_argument('--key',
                        dest='key',
                        help="system key file")
    parser.add_argument('--bearer-token',
                        dest='bearer_token',
                        help="bearer access token")

    parser.add_argument('--update-time',
                        dest='update_time',
                        help="update time stamp")
    parser.add_argument('--unbound-time',
                        dest='unbounded_time', action='store_true',
                        help="disable time sanity limits")

    parser.add_argument('--message',
                        dest='status_message',
                        help="status message text")

    parser.add_argument('--notifications',
                        dest='notifications', nargs='*',
                        help="notifications to set (CODE[:SEVERITY[:DATA]])")
    parser.add_argument('--preserve-notifications',
                        dest='preserve_existing_notifications', action='store_true', default=None,
                        help="preserve existing notifications instead of clearing them")
    parser.add_argument('--clear-notifications',
                        dest='notifications_to_clear', nargs='*',
                        help="notifications to clear (implies preserve)")

    parser.add_argument('--watchdogs',
                        dest='watchdogs', nargs='*',
                        help="watchdogs to (re-)start (CODE[:SEVERITY[:DATA]])")
    parser.add_argument('--clear-watchdogs',
                        dest='watchdogs_to_clear', nargs='*',
                        help="watchdogs to clear")

    parser.add_argument('--events',
                        dest='events', nargs='*',
                        help="events to add (CODE[:TIME[:SEVERITY[:DATA]]])")

    parser.add_argument('--conditions',
                        dest='conditions', nargs='*',
                        help="conditions to add (CODE[:START_TIME[:END_TIME[:SEVERITY[:DATA]]]])")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Forge dashboard reporting.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")

    subparsers = parser.add_subparsers(dest='command')

    command_parser = subparsers.add_parser('ok',
                                           help="report success")
    add_reporting_arguments(command_parser)
    command_parser.add_argument('code',
                                help="entry type code")
    command_parser.add_argument('station',
                                help="station code",
                                nargs='?')

    command_parser = subparsers.add_parser('failed',
                                           help="report failure")
    add_reporting_arguments(command_parser)
    command_parser.add_argument('code',
                                help="entry type code")
    command_parser.add_argument('station',
                                help="station code",
                                nargs='?')

    command_parser = subparsers.add_parser('update',
                                           help="update status without changing failure state")
    add_reporting_arguments(command_parser)
    command_parser.add_argument('code',
                                help="entry type code")
    command_parser.add_argument('station',
                                help="station code",
                                nargs='?')

    return parser.parse_args()


def populate_report_arguments(args) -> typing.Dict[str, typing.Any]:
    kwargs = dict()
    for a in ('url', 'key', 'bearer_token', 'update_time',
              'notifications', 'preserve_existing_notifications', 'notifications_to_clear',
              'watchdogs', 'watchdogs_to_clear',
              'events', 'conditions'):
        v = getattr(args, a, None)
        if v is None:
            continue
        kwargs[a] = v

    if args.status_message:
        notifications = kwargs.get('notifications')
        if not notifications:
            notifications = []
        kwargs['notifications'] = notifications

        notifications.append({
            'code': '',
            'data': args.status_message,
        })

    return kwargs


def main():
    args = parse_arguments()

    if args.debug:
        root_logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)-40s %(message)s')
        handler.setFormatter(formatter)
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)

    async def run():
        if args.command == 'ok':
            await report_ok(args.code, args.station,
                            unreported_exception=True,
                            **populate_report_arguments(args))
        elif args.command == 'failed':
            await report_failed(args.code, args.station,
                                unreported_exception=True,
                                **populate_report_arguments(args))
        elif args.command == 'update':
            await dashboard_report(args.code, args.station,
                                   unreported_exception=True,
                                   **populate_report_arguments(args))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.close()


if __name__ == '__main__':
    main()