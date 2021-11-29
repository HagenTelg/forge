import typing
import asyncio
import ipaddress
import aiohttp
import psutil
import socket
import time
from .command import command_output


async def add_external_address(telemetry: typing.Dict[str, typing.Any]) -> None:
    try:
        dig = await asyncio.create_subprocess_exec('dig', '-4', '+short', 'myip.opendns.com', '@resolver1.opendns.com',
                                                   stdout=asyncio.subprocess.PIPE,
                                                   stdin=asyncio.subprocess.DEVNULL)

        address = await dig.stdout.read()
        await dig.wait()
        if dig.returncode != 0:
            raise OSError
        address = address.decode('ascii')
        address = address.strip()
        if not address:
            raise ValueError
        address = str(ipaddress.ip_address(address))
        telemetry['public_address'] = address
        return
    except:
        pass

    for target in ('https://ifconfig.me/ip', 'https://myip.dnsomatic.com/'):
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(target) as resp:
                    if resp.status != 200:
                        raise ValueError
                    address = await resp.text()
                    address = address.strip()
                    if not address:
                        raise ValueError
                    address = str(ipaddress.ip_address(address))
                    telemetry['public_address'] = address
                    return
        except:
            pass


async def add_local_addresses(telemetry: typing.Dict[str, typing.Any]) -> None:
    telemetry['network_interfaces'] = {}
    for interface, addresses in psutil.net_if_addrs().items():
        if interface == 'lo':
            continue
        interface_data = {
            'IPv4': [],
            'IPv6': [],
        }
        telemetry['network_interfaces'][interface] = interface_data

        for addr in addresses:
            if addr.family == socket.AddressFamily.AF_INET:
                if not addr.address or not addr.netmask:
                    continue
                try:
                    addr = ipaddress.IPv4Interface(addr.address + '/' + addr.netmask)
                except:
                    continue
                interface_data['IPv4'].append(str(addr))

                if addr.is_global or 'local_address' not in telemetry:
                    telemetry['local_address'] = str(addr.ip)
            elif addr.family == socket.AddressFamily.AF_INET6:
                if not addr.address or not addr.netmask:
                    continue
                mask_length = 0
                for word in addr.netmask.split(':'):
                    if not word:
                        break
                    word = int(word, 16)
                    if word == 0:
                        break
                    word_length = (word & (-word)).bit_length()-1
                    mask_length += 16 - word_length
                try:
                    addr = ipaddress.IPv6Interface(addr.address + '/' + str(mask_length))
                except:
                    continue
                if addr.is_link_local:
                    continue
                interface_data['IPv6'].append(str(addr))

                if addr.is_global or 'local_address6' not in telemetry:
                    telemetry['local_address6'] = str(addr.ip)


class _InterfaceAccumulated:
    def __init__(self, rx, tx, now):
        self.rx = rx
        self.tx = tx
        self.time = now


_last_interface_accumulators: typing.Dict[str, _InterfaceAccumulated] = {}


async def add_network_rate(telemetry: typing.Dict[str, typing.Any]) -> None:
    interface_counters = psutil.net_io_counters(pernic=True, nowrap=True)
    now = time.time()

    need_delay = False
    for interface, counters in interface_counters.items():
        if interface == 'lo':
            continue
        if interface in _last_interface_accumulators:
            continue
        _last_interface_accumulators[interface] = _InterfaceAccumulated(counters.bytes_recv, counters.bytes_sent, now)
        need_delay = True
    if need_delay:
        await asyncio.sleep(0.5)
        interface_counters = psutil.net_io_counters(pernic=True, nowrap=True)
        now = time.time()

    total_rx = 0
    total_tx = 0
    for interface, counters in interface_counters.items():
        if interface == 'lo':
            continue
        accumulated = _last_interface_accumulators.get(interface)
        if not accumulated:
            continue
        dT = now - accumulated.time
        if dT <= 0.0:
            continue
        total_rx += (counters.bytes_recv - accumulated.rx) / dT
        total_tx += (counters.bytes_sent - accumulated.tx) / dT
        accumulated.rx = counters.bytes_recv
        accumulated.tx = counters.bytes_sent
        accumulated.time = now

    telemetry['network_rx'] = total_rx
    telemetry['network_tx'] = total_tx


async def add_network_configuration(telemetry: typing.Dict[str, typing.Any]) -> None:
    telemetry['network_configuration'] = {
        'address': await command_output('ip', 'addr', 'show'),
        'route': await command_output('ip', '-4', 'route', 'show'),
        'route6': await command_output('ip', '-6', 'route', 'show'),
        'nm_device': await command_output('nmcli', 'device', 'show', silent=True),
    }
