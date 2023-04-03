import typing
import sys
import datetime
from json import dump as json_dump
from forge.dashboard import Severity


_SEVERITY_SORT = {
    Severity.INFO: 0,
    Severity.WARNING: 1,
    Severity.ERROR: 2,
}


def sort_entries(sort_keys: typing.List[str], entries: typing.List[typing.Dict]):
    def assemble_key(value: typing.Dict):
        result = list()

        for key in sort_keys:
            if key in ('station', 'data'):
                result.append(value.get(key) or '')
            elif key == 'failed':
                result.append(0 if value.get(key) else 1)
            elif key == 'severity':
                result.append(_SEVERITY_SORT.get(value.get(key), 0))
            elif key == 'time':
                result.append(value.get(key) or value.get('last_seen') or value.get('occurred_at') or value.get('start_time'))
            elif key in ('notifications', 'watchdogs', 'events', 'conditions'):
                result.append(0)
            else:
                result.append(value.get(key) or '')

        return tuple(result)

    entries.sort(key=lambda value: value.get('id', 0))
    if len(sort_keys) > 0:
        entries.sort(key=assemble_key)
    for entry in entries:
        for detail_type in ('notifications', 'watchdogs', 'events', 'conditions'):
            details = entry.get(detail_type)
            if not details:
                continue

            details.sort(key=lambda value: value.get('id', 0))
            if len(sort_keys) > 0:
                details.sort(key=assemble_key)


def sort_access(sort_keys: typing.List[str], entries: typing.List[typing.Dict]):
    def assemble_key(value: typing.Dict):
        result = list()

        for key in sort_keys:
            result.append(value.get(key) or '')

        return tuple(result)

    entries.sort(key=lambda value: value.get('id', 0))
    if len(sort_keys) > 0:
        entries.sort(key=assemble_key)


def display_entries_json(users: typing.List[typing.Dict]):
    json_dump(users, sys.stdout, default=str)


def display_access_json(users: typing.List[typing.Dict]):
    json_dump(users, sys.stdout, default=str)


def display_entries_text(entries: typing.List[typing.Dict]):
    header_station = "Station"
    header_code = "Code"
    header_id = "ID"
    header_status = "Status"
    header_updated = "Updated"

    header_details_code = "Code"
    header_details_id = "ID"
    header_details_severity = "Severity"
    header_details_time = "Time"

    details_notifications = "Notifications"
    details_watchdogs = "Watchdogs"
    details_events = "Events"
    details_conditions = "Conditions"

    entry_column_widths = [
        len(header_station),
        len(header_code),
        len(header_id),
        len(header_status),
        len(header_updated),
    ]

    details_column_widths = [
        0,
        0,
        len(header_details_code),
        len(header_details_id),
        len(header_details_severity),
        len(header_details_time),
    ]

    for entry in entries:
        columns = list()
        entry['display_columns'] = columns

        columns.append((entry.get('station', "") or "").upper())
        columns.append(entry['code'])
        columns.append(str(entry['id']))
        if entry['failed']:
            columns.append("FAILED")
        else:
            columns.append("OK")
        columns.append(f"{entry['updated']:%Y-%m-%d}")

        for i in range(len(columns)):
            entry_column_widths[i] = max(entry_column_widths[i], len(columns[i]))

        notifications = entry.get('notifications')
        if notifications:
            details_column_widths[1] = max(details_column_widths[1], len(details_notifications))
            for add in notifications:
                columns = ['', '']
                add['display_columns'] = columns

                columns.append(add['code'])
                columns.append(str(add['id']))
                columns.append(add['severity'].name)
                columns.append('')

                for i in range(len(columns)):
                    details_column_widths[i] = max(details_column_widths[i], len(columns[i]))

        watchdogs = entry.get('watchdogs')
        if watchdogs:
            details_column_widths[1] = max(details_column_widths[1], len(details_watchdogs))
            for add in watchdogs:
                columns = ['', '']
                add['display_columns'] = columns

                columns.append(add['code'])
                columns.append(str(add['id']))
                columns.append(add['severity'].name)
                columns.append(f"{add['last_seen']:%Y-%m-%d}")

                for i in range(len(columns)):
                    details_column_widths[i] = max(details_column_widths[i], len(columns[i]))

        events = entry.get('events')
        if events:
            details_column_widths[1] = max(details_column_widths[1], len(details_events))
            for add in events:
                columns = ['', '']
                add['display_columns'] = columns

                columns.append(add['code'])
                columns.append(str(add['id']))
                columns.append(add['severity'].name)
                columns.append(f"{add['occurred_at']:%Y-%m-%d}")

                for i in range(len(columns)):
                    details_column_widths[i] = max(details_column_widths[i], len(columns[i]))

        conditions = entry.get('conditions')
        if conditions:
            details_column_widths[1] = max(details_column_widths[1], len(details_conditions))
            for add in conditions:
                columns = ['', '']
                add['display_columns'] = columns

                columns.append(add['code'])
                columns.append(str(add['id']))
                columns.append(add['severity'].name)
                columns.append(f"{add['start_time']:%Y-%m-%d}")

                for i in range(len(columns)):
                    details_column_widths[i] = max(details_column_widths[i], len(columns[i]))

    def print_columns(widths, *args):
        result = ''
        for i in range(len(args)):
            if len(result) > 0:
                result += '  '
            result += args[i].ljust(widths[i])
        print(result)

    print_columns(entry_column_widths, header_station, header_code, header_id, header_status, header_updated)
    if details_column_widths[1] > 0:
        details_column_widths[0] = max(details_column_widths[0], entry_column_widths[0])
        details_column_widths[1] = max(details_column_widths[1], entry_column_widths[1])

        print_columns(details_column_widths, '', '', header_details_code, header_details_id, header_details_severity,
                      header_details_time)
    for entry in entries:
        print_columns(entry_column_widths, *entry['display_columns'])

        notifications = entry.get('notifications')
        if notifications:
            print_columns(details_column_widths, '', "Notifications")
            for add in notifications:
                print_columns(details_column_widths, *add['display_columns'])

        watchdogs = entry.get('watchdogs')
        if watchdogs:
            print_columns(details_column_widths, '', "Watchdogs")
            for add in watchdogs:
                print_columns(details_column_widths, *add['display_columns'])

        events = entry.get('events')
        if events:
            print_columns(details_column_widths, '', "Events")
            for add in events:
                print_columns(details_column_widths, *add['display_columns'])

        conditions = entry.get('conditions')
        if conditions:
            print_columns(details_column_widths, '', "Conditions")
            for add in conditions:
                print_columns(details_column_widths, *add['display_columns'])


def display_access_text(access: typing.List[typing.Dict], access_field: str, header_access: str):
    header_id = "ID"
    header_station = "Station"
    header_code = "Code"

    column_widths = [
        len(header_access),
        len(header_id),
        len(header_station),
        len(header_code),
    ]

    for a in access:
        columns = list()
        a['display_columns'] = columns

        columns.append(a[access_field])
        columns.append(str(a['id']))
        columns.append((a.get('station', "") or "").upper())
        columns.append(a['code'])

        for i in range(len(columns)):
            column_widths[i] = max(column_widths[i], len(columns[i]))

    def print_columns(widths, *args):
        result = ''
        for i in range(len(args)):
            if len(result) > 0:
                result += '  '
            result += args[i].ljust(widths[i])
        print(result)

    print_columns(column_widths, header_access, header_id, header_station, header_code)
    for a in access:
        print_columns(column_widths, *a['display_columns'])
