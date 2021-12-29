import typing
import sys
from json import dump as json_dump


def sort_access(access: typing.List[typing.Dict]) -> None:
    def assemble_key(value: typing.Dict):
        return value['station'], value['public_key']

    access.sort(key=lambda value: value.get('id', 0))
    access.sort(key=assemble_key)


def display_json(data: typing.List[typing.Dict]) -> None:
    json_dump(data, sys.stdout, default=str)


def display_access_text(access: typing.List[typing.Dict]) -> None:
    header_station = "Station"
    header_public_key = "Public Key"
    header_id = "ID"
    header_access = [
        "/-- Acquisition",
        "|",
    ]

    column_widths = [
        len(header_station),
        len(header_public_key),
        len(header_id),
        0,
    ]
    for header in header_access:
        column_widths[-1] = max(column_widths[-1], len(header))

    for info in access:
        columns = list()
        info['display_columns'] = columns

        columns.append(info['station'].upper())
        columns.append(info['public_key'])
        columns.append(str(info['id']))

        def allowed_marker(allowed: typing.Optional[bool]) -> str:
            return "*" if allowed else "-"

        columns.append(
            allowed_marker(info.get('acquisition'))
        )

        for i in range(len(columns)):
            column_widths[i] = max(column_widths[i], len(columns[i]))

    def print_columns(*args):
        result = ''
        for i in range(len(args)):
            if len(result) > 0:
                result += '  '
            result += args[i].ljust(column_widths[i])
        print(result)

    for header in header_access[:-1]:
        print_columns("", "", "", header)
    print_columns(header_station, header_public_key, header_id, header_access[-1])
    for info in access:
        print_columns(*info['display_columns'])
