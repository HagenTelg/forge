import typing
import asyncio
import logging
import os
import struct
from json import dumps as to_json
from forge.tasks import wait_cancelable
from forge.const import MAX_I64
from .control import Controller
from .. import CONFIGURATION
from ..protocol import ServerDiagnosticRequest

_LOGGER = logging.getLogger(__name__)


class Diagnostics:
    def __init__(self, socket_path: str, control: Controller):
        self.socket_path = socket_path
        self.control = control
        self._server = None

    async def initialize(self) -> None:
        _LOGGER.info(f"Listening on diagnostics socket {self.socket_path}")
        try:
            os.unlink(self.socket_path)
        except OSError:
            pass
        self._server = await asyncio.start_unix_server(self.connection, path=self.socket_path)

    def shutdown(self) -> None:
        try:
            self._server.close()
        except:
            pass
        self._server = None
        try:
            os.unlink(self.socket_path)
        except OSError:
            pass

    def _list_connections(self) -> typing.Dict:
        result = dict()
        for uid, connection in self.control.active_connections:
            cdata = dict()
            cdata['identifier'] = connection.identifier
            cdata['name'] = connection.name
            cdata['intent_count'] = len(self.control.intent.get_held(connection))
            cdata['notify_listen_count'] = len(self.control.notify.get_listening(connection))
            cdata['notify_wait_count'] = len(self.control.notify.get_awaiting_send(connection)) + len(self.control.notify.get_awaiting_acknowledge(connection))
            cdata['transaction'] = connection.diagnostic_transaction_status

            result[uid] = cdata
        return result

    def _list_intents(self) -> typing.Dict:
        result = dict()
        for uid, connection in self.control.active_connections:
            cdata = list()
            intents = self.control.intent.get_held(connection)
            for intent in intents.values():
                cdata.append({
                    'key': intent.key,
                    'start': intent.start,
                    'end': intent.end,
                })

            result[uid] = cdata
        return result

    def _list_locks(self) -> typing.Dict:
        result = dict()
        for uid, connection in self.control.active_connections:
            cdata = list()
            for lock in connection.diagnostic_transaction_locks:
                cdata.append({
                    'key': lock.key,
                    'start': lock.start,
                    'end': lock.end,
                    'type': 'write' if lock.write else 'read',
                })

            result[uid] = cdata
        return result

    def _list_notification_listeners(self) -> typing.Dict:
        result = dict()
        for uid, connection in self.control.active_connections:
            result[uid] = list(self.control.notify.get_listening(connection))
        return result

    def _list_notification_wait(self) -> typing.Dict:
        result = dict()
        for uid, connection in self.control.active_connections:
            pending = list()
            for p in self.control.notify.get_awaiting_send(connection):
                pending.append({
                    'status': 'send',
                    'key': p.key,
                    'start': p.start,
                    'end': p.end,
                })
            for p in self.control.notify.get_awaiting_acknowledge(connection).values():
                pending.append({
                    'status': 'acknowledge',
                    'key': p.key,
                    'start': p.start,
                    'end': p.end,
                })

            result[uid] = pending
        return result

    def _transaction_details(self, uid: int) -> typing.Dict:
        connection = self.control.active_connections.get(uid)
        if not connection:
            return dict()
        details = connection.diagnostic_transaction_details
        if not details:
            return dict()
        return details

    async def connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        _LOGGER.debug("Accepted diagnostic connection")
        try:
            request = await wait_cancelable(reader.readexactly(1), 10.0)
            request = ServerDiagnosticRequest(struct.unpack('<B', request)[0])
            _LOGGER.debug("Diagnostic request %s", request.name)
            if request == ServerDiagnosticRequest.LIST_CONNECTIONS:
                result = self._list_connections()
                writer.write(to_json(result).encode('utf-8'))
                await writer.drain()
            elif request == ServerDiagnosticRequest.LIST_INTENTS:
                result = self._list_intents()
                writer.write(to_json(result).encode('utf-8'))
                await writer.drain()
            elif request == ServerDiagnosticRequest.LIST_LOCKS:
                result = self._list_locks()
                writer.write(to_json(result).encode('utf-8'))
                await writer.drain()
            elif request == ServerDiagnosticRequest.LIST_NOTIFICATION_LISTENERS:
                result = self._list_notification_listeners()
                writer.write(to_json(result).encode('utf-8'))
                await writer.drain()
            elif request == ServerDiagnosticRequest.LIST_NOTIFICATION_WAIT:
                result = self._list_notification_wait()
                writer.write(to_json(result).encode('utf-8'))
                await writer.drain()
            elif request == ServerDiagnosticRequest.TRANSACTION_DETAILS:
                uid = await wait_cancelable(reader.readexactly(8), 10.0)
                uid = struct.unpack('<Q', uid)[0]
                result = self._transaction_details(uid)
                writer.write(to_json(result).encode('utf-8'))
                await writer.drain()
            elif request == ServerDiagnosticRequest.CLOSE_CONNECTION:
                uid = await wait_cancelable(reader.readexactly(8), 10.0)
                uid = struct.unpack('<Q', uid)[0]
                connection = self.control.active_connections.get(uid)
                if connection:
                    try:
                        connection.writer.close()
                    except:
                        pass
                    writer.write(b"OK")
                else:
                    writer.write(b"CONNECTION NOT FOUND")
            else:
                raise ValueError
        except:
            _LOGGER.debug("Error in diagnostic connection", exc_info=True)
        finally:
            try:
                writer.close()
            except OSError:
                pass


def main():
    import argparse
    import time

    parser = argparse.ArgumentParser(description="Forge archive server diagnostics.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--socket',
                        dest='socket', default=CONFIGURATION.get('ARCHIVE.DIAGNOSTIC_SOCKET'),
                        help="set the diagnostic control socket")

    subparsers = parser.add_subparsers(dest='command')

    command_parser = subparsers.add_parser('connections',
                                           help="list open connections")
    command_parser.add_argument('--json',
                                dest='json', action='store_true',
                                help="output entry list in JSON")

    command_parser = subparsers.add_parser('close-connection',
                                           help="close an active connection")
    command_parser.add_argument('uid',
                                type=int,
                                help="the connection UID to close")

    command_parser = subparsers.add_parser('intents',
                                           help="list active intents")
    command_parser.add_argument('--json',
                                dest='json', action='store_true',
                                help="output entry list in JSON")
    command_parser.add_argument('uid',
                                type=int, nargs='?',
                                help="the connection UID to list intents for")

    command_parser = subparsers.add_parser('locks',
                                           help="list active locks")
    command_parser.add_argument('--json',
                                dest='json', action='store_true',
                                help="output entry list in JSON")
    command_parser.add_argument('uid',
                                type=int, nargs='?',
                                help="the connection UID to list locks for")

    command_parser = subparsers.add_parser('notification-listeners',
                                           help="show notification listeners")
    command_parser.add_argument('--json',
                                dest='json', action='store_true',
                                help="output entry list in JSON")
    command_parser.add_argument('uid',
                                type=int, nargs='?',
                                help="the connection UID to display")

    command_parser = subparsers.add_parser('notification-awaiting',
                                           help="show notification awaiting processing")
    command_parser.add_argument('--json',
                                dest='json', action='store_true',
                                help="output entry list in JSON")
    command_parser.add_argument('uid',
                                type=int, nargs='?',
                                help="the connection UID to display")

    command_parser = subparsers.add_parser('transaction',
                                           help="get active transaction details")
    command_parser.add_argument('--json',
                                dest='json', action='store_true',
                                help="output entry list in JSON")
    command_parser.add_argument('uid',
                                type=int,
                                help="the connection UID to display")

    args = parser.parse_args()
    if not args.socket:
        parser.error("No diagnostic socket set")
    if args.debug:
        root_logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)-40s %(message)s')
        handler.setFormatter(formatter)
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)

    def output_columns(headers: typing.List[str], rows: typing.List[typing.List[str]],
                       flex: int = None, prefix: str = "") -> None:
        column_widths = [len(h) for h in headers]
        for r in rows:
            for c in range(len(r)):
                while c >= len(column_widths):
                    column_widths.append(0)
                column_widths[c] = max(column_widths[c], len(r[c]))

        # Start padding for spacing
        for c in range(1, len(column_widths)):
            width = column_widths[c]
            if width:
                width += 2
            column_widths[c] = width

        if flex is not None:
            terminal_width = 0
            try:
                check = os.get_terminal_size()
                terminal_width = check.columns
            except:
                pass
            if terminal_width > 0 and sum(column_widths)+len(prefix) >= terminal_width:
                reclaim_width = ((sum(column_widths)+len(prefix)) - terminal_width) + 1
                column_widths[flex] = max(min(column_widths[flex], 8), column_widths[flex] - reclaim_width)

        def output_row(data: typing.List[str]) -> None:
            d = ""
            for c in range(len(column_widths)):
                v = data[c] if c < len(data) else ""
                if c == 0:
                    d += v.ljust(column_widths[c])
                else:
                    d += "  " + v.ljust(column_widths[c] - 1)
            print(prefix + d)

        output_row(headers)
        for r in rows:
            output_row(r)

    def format_time(value: int) -> str:
        if value <= -MAX_I64:
            return "-∞"
        if value >= MAX_I64:
            return "+∞"
        ts = time.gmtime(value / 1000.0)
        return f"{ts.tm_year:04}-{ts.tm_mon:02}-{ts.tm_mday}T{ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}"

    async def run():
        reader, writer = await asyncio.open_unix_connection(args.socket)

        async def read_all_json():
            from json import loads as from_json
            data = await reader.read()
            return from_json(data)

        if args.command == 'connections':
            writer.write(struct.pack('<B', ServerDiagnosticRequest.LIST_CONNECTIONS.value))
            response = await read_all_json()
            if args.json:
                print(to_json(response))
            else:
                output_rows = list()
                for uid in sorted(response.keys()):
                    connection = response[uid]
                    data = [
                        str(uid),
                        connection['identifier'],
                        connection['name'],
                        f"{connection['intent_count']:d}/{connection['notify_listen_count']:d}/{connection['notify_wait_count']:d}",
                    ]
                    transaction = connection.get('transaction')
                    if transaction:
                        rw = "W" if transaction['type'] == 'read' else "W"
                        elapsed = time.time() - transaction['begin'] / 1000.0
                        status = transaction['status'] or ""
                        data.append(f"{rw}@{transaction['generation']:d}%{transaction['lock_count']:d} {elapsed:.0f}: {status}")
                    output_rows.append(data)

                output_columns([
                    "UID", "SOURCE", "NAME", "I/N/W", "TRANSACTION"
                ], output_rows, flex=-1)
        elif args.command == 'close-connection':
            uid = args.uid
            writer.write(struct.pack('<BQ', ServerDiagnosticRequest.CLOSE_CONNECTION.value, uid))
            data = await reader.read()
            print(data.decode('utf-8'))
        elif args.command == 'intents':
            writer.write(struct.pack('<B', ServerDiagnosticRequest.LIST_INTENTS.value))
            response = await read_all_json()
            if args.uid:
                response = response.get(args.uid, [])
                if args.json:
                    print(to_json(response))
                else:
                    output_rows = list()
                    response.sort(key=lambda x: (x['key'], x['start'], x['end']))
                    for intent in response:
                        output_rows.append([
                            intent['key'],
                            format_time(intent['start']),
                            format_time(intent['end']),
                        ])
                    output_columns([
                        "KEY", "START", "END"
                    ], output_rows)
            else:
                if args.json:
                    print(to_json(response))
                else:
                    all_intents = list()
                    for uid, intents in response.items():
                        for intent in intents:
                            all_intents.append((
                                uid,
                                intent['key'],
                                intent['start'],
                                intent['end']
                            ))
                    all_intents.sort(key=lambda x: (x[1], x[2], x[0], x[3]))
                    output_rows = list()
                    for intent in all_intents:
                        output_rows.append([
                            str(intent[0]),
                            intent[1],
                            format_time(intent[2]),
                            format_time(intent[3]),
                        ])
                    output_columns([
                        "UID", "KEY", "START", "END"
                    ], output_rows, flex=1)
        elif args.command == 'locks':
            writer.write(struct.pack('<B', ServerDiagnosticRequest.LIST_LOCKS.value))
            response = await read_all_json()
            if args.uid:
                response = response.get(args.uid, [])
                if args.json:
                    print(to_json(response))
                else:
                    output_rows = list()
                    response.sort(key=lambda x: (x['key'], x['start'], x['end']))
                    for lock in response:
                        output_rows.append([
                            lock['key'],
                            "W" if lock['type'] == 'write' else "R",
                            format_time(lock['start']),
                            format_time(lock['end']),
                        ])
                    output_columns([
                        "KEY", "R/W", "START", "END"
                    ], output_rows)
            else:
                if args.json:
                    print(to_json(response))
                else:
                    all_locks = list()
                    for uid, locks in response.items():
                        for lock in locks:
                            all_locks.append((
                                uid,
                                lock['key'],
                                lock['type'],
                                lock['start'],
                                lock['end'],
                            ))
                    all_locks.sort(key=lambda x: (x[1], x[3], x[0], x[4]))
                    output_rows = list()
                    for lock in all_locks:
                        output_rows.append([
                            str(lock[0]),
                            lock[1],
                            "W" if lock[2] == 'write' else "R",
                            format_time(lock[3]),
                            format_time(lock[4]),
                        ])
                    output_columns([
                        "UID", "KEY", "R/W", "START", "END"
                    ], output_rows, flex=1)
        elif args.command == 'notification-listeners':
            writer.write(struct.pack('<B', ServerDiagnosticRequest.LIST_NOTIFICATION_LISTENERS.value))
            response = await read_all_json()
            if args.uid:
                response = response.get(args.uid, [])
                if args.json:
                    print(to_json(response))
                else:
                    for l in sorted(response):
                        print(l)
            else:
                if args.json:
                    print(to_json(response))
                else:
                    all_listen = dict()
                    for uid, listeners in response.items():
                        for listen in listeners:
                            target = all_listen.get(listen)
                            if not target:
                                target = set()
                                all_listen[listen] = target
                            target.add(uid)

                    output_rows = list()
                    for key in sorted(all_listen.keys()):
                        output_rows.append([
                            str(key),
                            ", ".join([str(uid) for uid in sorted(all_listen[key])])
                        ])
                    output_columns([
                        "KEY", "UIDS"
                    ], output_rows, flex=-1)
        elif args.command == 'notification-awaiting':
            writer.write(struct.pack('<B', ServerDiagnosticRequest.LIST_NOTIFICATION_WAIT.value))
            response = await read_all_json()
            if args.uid:
                response = response.get(args.uid, [])
                if args.json:
                    print(to_json(response))
                else:
                    output_rows = list()
                    response.sort(key=lambda x: (x['key'], 0 if x['status'] == 'send' else 1, x['start'], x['end']))
                    for pending in response:
                        output_rows.append([
                            pending['key'],
                            "SEND" if pending['status'] == 'send' else "WAIT",
                            format_time(pending['start']),
                            format_time(pending['end']),
                        ])
                    output_columns([
                        "KEY", "STATE", "START", "END"
                    ], output_rows)
            else:
                if args.json:
                    print(to_json(response))
                else:
                    all_pending = list()
                    for uid, data in response.items():
                        for pending in data:
                            all_pending.append((
                                uid,
                                pending['key'],
                                pending['status'],
                                pending['start'],
                                pending['end'],
                            ))

                    all_pending.sort(key=lambda x: (x[1], x[3], x[0], x[4]))
                    output_rows = list()
                    for pending in all_pending:
                        output_rows.append([
                            str(pending[0]),
                            pending[1],
                            "SEND" if pending[2] == 'send' else "PROCESS",
                            format_time(pending[3]),
                            format_time(pending[4]),
                        ])
                    output_columns([
                        "UID", "KEY", "STATE", "START", "END"
                    ], output_rows, flex=1)
        elif args.command == 'transaction':
            writer.write(struct.pack('<BQ', ServerDiagnosticRequest.TRANSACTION_DETAILS.value, args.uid))
            response = await read_all_json()
            if args.json:
                print(to_json(response))
            elif not response:
                print("NOT IN TRANSACTION")
            else:
                elapsed = time.time() - response['begin'] / 1000.0
                if response['type'] == 'read':
                    print(f"READ-ONLY TRANSACTION STARTED AT {format_time(response['begin'])} ({elapsed:.2f} S)")
                else:
                    print(f"READ-WRITE TRANSACTION STARTED AT {format_time(response['begin'])} ({elapsed:.2f} S)")
                print(f"STATUS: {response['status']}")
                print(f"STORAGE GENERATION: {response['generation']}")

                locks = response['locks']
                if locks:
                    output_rows = list()
                    locks.sort(key=lambda x: (x['key'], x['start'], x['end']))
                    for lock in locks:
                        output_rows.append([
                            lock['key'],
                            "W" if lock['type'] == 'write' else "R",
                            format_time(lock['start']),
                            format_time(lock['end']),
                        ])
                    print("LOCKS HELD:")
                    output_columns([
                        "KEY", "R/W", "START", "END"
                    ], output_rows, flex=0, prefix="    ")

                notifications = response['notifications']
                if notifications:
                    output_rows = list()
                    notifications.sort(key=lambda x: (x['key'], x['start'], x['end']))
                    for notification in notifications:
                        output_rows.append([
                            notification['key'],
                            format_time(notification['start']),
                            format_time(notification['end']),
                        ])
                    print("NOTIFICATIONS SEND ON COMMIT:")
                    output_columns([
                        "KEY", "START", "END"
                    ], output_rows, flex=0, prefix="    ")

                intents = response['intent_release']
                if intents:
                    output_rows = list()
                    intents.sort(key=lambda x: (x['key'], x['start'], x['end']))
                    for intent in intents:
                        output_rows.append([
                            intent['key'],
                            format_time(intent['start']),
                            format_time(intent['end']),
                        ])
                    print("INTENTS RELEASED ON COMMIT:")
                    output_columns([
                        "KEY", "START", "END"
                    ], output_rows, flex=0, prefix="    ")

                intents = response['intent_acquire']
                if intents:
                    output_rows = list()
                    intents.sort(key=lambda x: (x['key'], x['start'], x['end']))
                    for intent in intents:
                        output_rows.append([
                            intent['key'],
                            format_time(intent['start']),
                            format_time(intent['end']),
                        ])
                    print("INTENTS ACQUIRED ON COMMIT:")
                    output_columns([
                        "KEY", "START", "END"
                    ], output_rows, flex=0, prefix="    ")
        try:
            writer.close()
        except OSError:
            pass

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.close()


if __name__ == '__main__':
    main()
