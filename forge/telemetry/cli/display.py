import typing
import sys
import datetime
import re
from math import isfinite
from json import dump as json_dump


def sort_hosts(sort_keys: typing.List[str], hosts: typing.List[typing.Dict]) -> None:
    def assemble_key(value: typing.Dict):
        result = list()

        for key in sort_keys:
            if key == 'last_seen':
                return value.get(key, datetime.datetime.min)
            return value.get(key, '')

        return tuple(result)

    hosts.sort(key=lambda value: value.get('id', 0))
    if len(sort_keys) > 0:
        hosts.sort(key=assemble_key)


def sort_access(access: typing.List[typing.Dict]) -> None:
    def assemble_key(value: typing.Dict):
        return value['station'], value['public_key']

    access.sort(key=lambda value: value.get('id', 0))
    access.sort(key=assemble_key)


def display_json(hosts: typing.List[typing.Dict]) -> None:
    json_dump(hosts, sys.stdout, default=str)


def display_hosts_text(hosts: typing.List[typing.Dict]) -> None:
    header_id = "ID"
    header_public_key = "Public Key"
    header_station = "Station"
    header_last_seen = "Last Seen"
    header_remote_address = "Address"

    column_widths = [
        len(header_public_key),
        len(header_id),
        len(header_station),
        len(header_last_seen),
        len(header_remote_address),
    ]

    for host in hosts:
        columns = list()
        host['display_columns'] = columns

        columns.append(host['public_key'])
        columns.append(str(host['id']))
        columns.append((host.get('station') or "").upper())
        columns.append(f"{host['last_seen']:%Y-%m-%d}")
        columns.append(host.get('remote_host', ''))

        for i in range(len(columns)):
            column_widths[i] = max(column_widths[i], len(columns[i]))

    def print_columns(widths, *args):
        result = ''
        for i in range(len(args)):
            if len(result) > 0:
                result += '  '
            result += args[i].ljust(widths[i])
        print(result)

    print_columns(column_widths, header_public_key, header_id, header_station, header_last_seen, header_remote_address)
    for host in hosts:
        print_columns(column_widths, *host['display_columns'])


def _format_bytes(n: float) -> str:
    if not isfinite(n):
        return "---B"
    divisor = 1
    for u in ("B", "KiB", "MiB", "GiB", "TiB"):
        divided = n / divisor
        if divided > 999.0:
            divisor *= 1024
            continue

        if divisor == 1:
            return f"{divided:.0f} {u}"
        elif divided <= 9.99:
            return f"{divided:.2f} {u}"
        elif divided <= 99.9:
            return f"{divided:.1f} {u}"
        else:
            return f"{divided:.0f} {u}"


def _display_log_text(events: typing.List[typing.Dict[str, typing.Any]], include_source=True,
                      prefix="") -> None:
    if not events:
        return
    for e in events:
        dt = datetime.datetime.fromtimestamp(e['time'], datetime.timezone.utc)
        line = f"{prefix}{dt:%Y-%m-%d %H:%M:%S}"
        if include_source:
            line += " " + e.get('source', "")
        line += ": " + e.get('message', "")
        print(line)


def _prefix_output(output: str, prefix) -> None:
    if not output:
        return
    output = output.replace('\r\n', '\n')
    output = output.replace('\n\r', '\n')
    output = output.replace('\r', '\n')
    output = re.sub(r'\n{2,}', '\n\n', output)
    for line in output.split('\n'):
        print(f"{prefix}{line}")


def display_details_text(hosts: typing.List[typing.Dict]) -> None:
    first = True
    for host in hosts:
        if not first:
            print("\n\n")
        first = False

        print(f"****** {host['public_key']} ({host['id']}) {(host.get('station') or '').upper()} ******")
        print(f"Last seen: {host['last_seen']:%Y-%m-%d %H:%M:%S}")
        if host.get('boot_time'):
            dt = datetime.datetime.fromtimestamp(host['boot_time'], datetime.timezone.utc)
            print(f"Boot time: {dt:%Y-%m-%d %H:%M:%S}")
        if host.get('lsb'):
            print(f"Distribution: {host['lsb'].get('Distributor ID', '')} - {host['lsb'].get('Release', '')}")
        if host.get('remote_host'):
            print(f"Public Address: {host['remote_host']}")
        if host['last_update'].get('telemetry'):
            print(f"Auxiliary telemetry updated: {host['last_update']['telemetry']:%Y-%m-%d %H:%M:%S}")
        if host.get('time_offset'):
            print(f"Time offset (seconds): {host['time_offset']}")
            print(f"              Updated: {host['last_update']['time_offset']:%Y-%m-%d %H:%M:%S}")
        if host.get('public_address') or host.get('local_address') or host.get('local_address6'):
            print(f"Remote address: {host.get('public_address')}")
            if host.get('local_address'):
                print(f"    Local IPv4: {host['local_address']}")
            if host.get('local_address6'):
                print(f"    Local IPv6: {host['local_address6']}")
            print(f"       Updated: {host['last_update']['address']:%Y-%m-%d %H:%M:%S}")
        if host.get('login_user'):
            print(f"Local login user: {host['login_user']}")
            print(f"         Updated: {host['last_update']['login']:%Y-%m-%d %H:%M:%S}")
        if host.get('memory_utilization'):
            print(f"RAM: {host['memory_utilization'].get('usage_percent'):.1f}% of {_format_bytes(host['memory_utilization'].get('total_bytes'))}")
            print(f"Swap: {host['memory_utilization'].get('swap_percent'):.1f}% of {_format_bytes(host['memory_utilization'].get('swap_bytes'))}")
        if host.get('root_total_bytes') or host.get('root_used_percent'):
            print(f"Disk space: {host.get('root_used_percent'):.1f}% of {_format_bytes(host.get('root_total_bytes'))}")
        if host.get('disk_read') or host.get('disk_write'):
            print(f"Disk utilization: Read {_format_bytes(host.get('disk_read'))}/s, Write {_format_bytes(host.get('disk_write'))}/s")
        if host.get('network_rx') or host.get('network_tx'):
            print(f"Network: Rx {_format_bytes(host.get('network_rx'))}/s, Tx {_format_bytes(host.get('network_tx'))}/s")
        if host.get('cpu_total_utilization') or host.get('cpu_core_utilization'):
            print(f"CPU Utilization: {host['cpu_total_utilization']:.1f}% Total, {host['cpu_core_utilization']:.1f}% Core")
        if host.get('cpu_temperature'):
            print(f"CPU Temperature: {host['cpu_temperature']:.1f} °C")

        if host.get('ntp'):
            if host['ntp'].get('chrony_status'):
                print("NTP (chrony) status:")
                _prefix_output(host['ntp']['chrony_status'], prefix="    ")
                _prefix_output(host['ntp'].get('chrony_tracking'), prefix="    ")
                _prefix_output(host['ntp'].get('chrony_sourcestats'), prefix="    ")
            elif host['ntp'].get('ntpd_peers'):
                print("NTP (ntpd) status:")
                _prefix_output(host['ntp']['ntpd_peers'], prefix="    ")
                _prefix_output(host['ntp'].get('ntpd_vars'), prefix="    ")
            elif host['ntp'].get('timedatectl_status'):
                print("NTP (systemd) status:")
                _prefix_output(host['ntp']['timedatectl_status'], prefix="    ")

        if host.get('failed_services'):
            failed_services = host['failed_services']
            if len(failed_services) > 0:
                print("Failed services:")
                for service in failed_services:
                    _prefix_output(f"{service['name']} ({service['sub_state']}) - {service['description']}",
                                   prefix="    ")

        if host.get('log_acquisition'):
            print(f"Acquisition log updated at {host['last_update']['log_acquisition']:%Y-%m-%d %H:%M:%S}:")
            _display_log_text(host['log_acquisition'][-10:], prefix="    ")
        if host.get('log_kernel'):
            print(f"Kernel log updated at {host['last_update']['log_kernel']:%Y-%m-%d %H:%M:%S}:")
            _display_log_text(host['log_kernel'][-10:], include_source=False, prefix="    ")

        if host.get('network_configuration'):
            print("Network configuration:")
            if host['network_configuration'].get('address'):
                _prefix_output(host['network_configuration']['address'], prefix="    ")
            if host['network_configuration'].get('route'):
                print(" Routes (IPv4):")
                _prefix_output(host['network_configuration']['route'], prefix="    ")
            if host['network_configuration'].get('route6'):
                print(" Routes (IPv6):")
                _prefix_output(host['network_configuration']['route6'], prefix="    ")
            if host['network_configuration'].get('nm_device'):
                print(" Network interface settings:")
                _prefix_output(host['network_configuration']['nm_device'], prefix="    ")
            if host['network_configuration'].get('nm_connections'):
                for connection in host['network_configuration']['nm_connections']:
                    print(" Network interface connection:")
                    _prefix_output(connection, prefix="    ")


def display_login_text(access: typing.List[typing.Dict]) -> None:
    header_public_key = "Public Key"
    header_station = "Station"
    header_login_user = "User"
    header_public_address = "Public Address"
    header_local_address = "Local Address"

    column_widths = [
        len(header_public_key),
        len(header_station),
        len(header_login_user),
        len(header_public_address),
        len(header_local_address),
    ]

    for a in access:
        columns = list()
        a['display_columns'] = columns

        columns.append(a['public_key'])
        columns.append((a.get('station') or "").upper())
        columns.append(a.get('login_user') or "")
        columns.append(a.get('remote_host') or a.get('public_address') or "")
        columns.append(a.get('local_address') or a.get('local_address6') or "")

        for i in range(len(columns)):
            column_widths[i] = max(column_widths[i], len(columns[i]))

    def print_columns(widths, *args):
        result = ''
        for i in range(len(args)):
            if len(result) > 0:
                result += '  '
            result += args[i].ljust(widths[i])
        print(result)

    print_columns(column_widths, header_public_key, header_station, header_login_user,
                  header_public_address, header_local_address)
    for a in access:
        print_columns(column_widths, *a['display_columns'])


def display_access_text(access: typing.List[typing.Dict]) -> None:
    header_public_key = "Public Key"
    header_station = "Station"

    column_widths = [
        len(header_public_key),
        len(header_station),
    ]

    for a in access:
        columns = list()
        a['display_columns'] = columns

        columns.append(a['public_key'])
        columns.append((a.get('station') or "").upper())

        for i in range(len(columns)):
            column_widths[i] = max(column_widths[i], len(columns[i]))

    def print_columns(widths, *args):
        result = ''
        for i in range(len(args)):
            if len(result) > 0:
                result += '  '
            result += args[i].ljust(widths[i])
        print(result)

    print_columns(column_widths, header_public_key, header_station)
    for a in access:
        print_columns(column_widths, *a['display_columns'])
