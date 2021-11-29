import typing
import sys
import datetime
from json import dump as json_dump


def sort_users(sort_keys: typing.List[str], users: typing.List[typing.Dict]):
    def assemble_key(value: typing.Dict):
        result = list()

        for key in sort_keys:
            if key == 'last_seen':
                return value.get(key, datetime.datetime.min)
            elif key == 'access':
                return 0
            return value.get(key, '')

        return tuple(result)

    users.sort(key=lambda value: value.get('id', 0))
    if len(sort_keys) > 0:
        users.sort(key=assemble_key)
    for user in users:
        user['access'].sort(key=lambda value: value.get('station', ''))
        if len(sort_keys) > 0:
            user['access'].sort(key=assemble_key)


def display_users_json(users: typing.List[typing.Dict]):
    json_dump(users, sys.stdout, default=str)


def display_users_text(users: typing.List[typing.Dict]):
    header_name = "Name"
    header_id = "ID"
    header_last_seen = "Last Seen"
    header_initials = "Initials"

    header_station = "Station"
    header_mode = "Mode"
    header_read_only = "RO"

    user_column_widths = [
        len(header_name),
        len(header_id),
        len(header_last_seen),
        len(header_initials),
    ]
    access_column_widths = [
        0,
        len(header_station),
        len(header_mode),
        len(header_read_only),
    ]

    for user in users:
        columns = list()
        user['display_columns'] = columns

        if len(user.get('name', '')) > 0 and len(user.get('email', '')) > 0:
            columns.append(f"{user['name']} <{user['email']}>")
        elif len(user.get('name', '')) > 0:
            columns.append(user['name'])
        else:
            columns.append(user['email'])

        columns.append(str(user['id']))

        last_seen = user.get('last_seen')
        if last_seen is not None:
            columns.append(f'{last_seen:%Y-%m-%d}')
        else:
            columns.append('')

        columns.append(user.get('initials', ''))

        for i in range(len(columns)):
            user_column_widths[i] = max(user_column_widths[i], len(columns[i]))

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
    print_columns(access_column_widths, '', header_station, header_mode, header_read_only)
    for user in users:
        print_columns(user_column_widths, *user['display_columns'])
        for access in user['access']:
            print_columns(access_column_widths, *access['display_columns'])
