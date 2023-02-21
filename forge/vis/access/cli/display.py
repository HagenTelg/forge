import typing
import sys
import datetime
from json import dump as json_dump
from forge.vis.access.database import SubscriptionLevel


_LEVEL_SORT = {
    SubscriptionLevel.OFF: 0,
    SubscriptionLevel.INFO: 1,
    SubscriptionLevel.WARNING: 2,
    SubscriptionLevel.ERROR: 3,
    SubscriptionLevel.ALWAYS: 4,
}


def sort_users(sort_keys: typing.List[str], users: typing.List[typing.Dict]):
    def assemble_key(value: typing.Dict):
        result = list()

        for key in sort_keys:
            if key == 'last_seen':
                result.append(value.get(key) or datetime.datetime.min)
            elif key == 'access':
                result.append(0)
            else:
                result.append(value.get(key) or '')

        return tuple(result)

    users.sort(key=lambda value: value.get('id', 0))
    if len(sort_keys) > 0:
        users.sort(key=assemble_key)
    for user in users:
        user['access'].sort(key=lambda value: value.get('station', ''))
        if len(sort_keys) > 0:
            user['access'].sort(key=assemble_key)
        user['subscriptions'].sort(key=lambda value: (value.get('station', ''),
                                                      value.get('code', ''),
                                                      _LEVEL_SORT[value.get('level', SubscriptionLevel.OFF)]))
        if len(sort_keys) > 0:
            user['subscriptions'].sort(key=assemble_key)


def display_users_json(users: typing.List[typing.Dict]):
    json_dump(users, sys.stdout, default=str)


def display_users_text(users: typing.List[typing.Dict], subscriptions: bool = False):
    header_name = "Name"
    header_id = "ID"
    header_last_seen = "Last Seen"
    header_initials = "Initials"

    header_access_station = "Station"
    if subscriptions:
        header_access_type = "Code"
        header_access_level = "Level"
    else:
        header_access_type = "Mode"
        header_access_level = "RO"

    user_column_widths = [
        len(header_name),
        len(header_id),
        len(header_last_seen),
        len(header_initials),
    ]
    access_column_widths = [
        0,
        len(header_access_station),
        len(header_access_type),
        len(header_access_level),
    ]

    for user in users:
        columns = list()
        user['display_columns'] = columns

        if user.get('name') and user.get('email'):
            columns.append(f"{user['name']} <{user['email']}>")
        elif user.get('name'):
            columns.append(user['name'])
        else:
            columns.append(user['email'])

        columns.append(str(user['id']))

        last_seen = user.get('last_seen')
        if last_seen is not None:
            columns.append(f'{last_seen:%Y-%m-%d}')
        else:
            columns.append('')

        columns.append(user.get('initials') or '')

        for i in range(len(columns)):
            user_column_widths[i] = max(user_column_widths[i], len(columns[i]))

        if subscriptions:
            for sub in user['subscriptions']:
                columns = ['']
                sub['display_columns'] = columns

                columns.append(sub['station'].upper())
                columns.append(sub['code'])
                columns.append(sub['level'].name)

                for i in range(len(columns)):
                    access_column_widths[i] = max(access_column_widths[i], len(columns[i]))
        else:
            for access in user['access']:
                columns = ['']
                access['display_columns'] = columns

                columns.append(access['station'].upper())
                columns.append(access['mode'])
                columns.append('R' if not access['write'] else '')

                for i in range(len(columns)):
                    access_column_widths[i] = max(access_column_widths[i], len(columns[i]))

    access_column_widths[0] = max(access_column_widths[0], user_column_widths[0] + user_column_widths[1] + 2)

    def print_columns(widths, *args):
        result = ''
        for i in range(len(args)):
            if len(result) > 0:
                result += '  '
            result += args[i].ljust(widths[i])
        print(result)

    print_columns(user_column_widths, header_name, header_id, header_last_seen, header_initials)
    print_columns(access_column_widths, '', header_access_station, header_access_type, header_access_level)
    for user in users:
        print_columns(user_column_widths, *user['display_columns'])
        if subscriptions:
            for sub in user['subscriptions']:
                print_columns(access_column_widths, *sub['display_columns'])
        else:
            for access in user['access']:
                print_columns(access_column_widths, *access['display_columns'])
