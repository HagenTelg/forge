import typing
import asyncio
import psutil
from .command import command_output, command_lines


async def add_system_info(telemetry: typing.Dict[str, typing.Any]) -> None:
    telemetry['boot_time'] = psutil.boot_time()
    telemetry['cpu_cores'] = psutil.cpu_count()

    telemetry['users'] = []
    for user in psutil.users():
        telemetry['users'].append({
            'name': user.name,
            'terminal': user.terminal,
            'host': user.host,
            'session_start': user.started,
        })

    processes = []
    for p in psutil.process_iter(['pid', 'username', 'create_time',
                                  'cpu_percent', 'memory_percent',
                                  'name', 'exe', 'cmdline']):
        processes.append(p.info)

    def sort_key(process):
        memory = process.get('memory_percent', 0)
        cpu = process.get('cpu_percent', 0)
        userspace = (process.get('exe', None) is not None)
        return int(memory/10), int(cpu/10), userspace

    processes.sort(key=sort_key)
    del processes[:-30]
    telemetry['processes'] = processes


async def add_lsb_info(telemetry: typing.Dict[str, typing.Any]) -> None:
    data = {}
    for line in await command_lines('lsb_release', '-a', silent=True):
        fields = line.split(':', 2)
        if len(fields) < 2:
            continue
        data[fields[0].strip()] = fields[1].strip()
    if len(data) == 0:
        return

    telemetry['lsb'] = data


async def add_uname(telemetry: typing.Dict[str, typing.Any]) -> None:
    telemetry['uname'] = await command_output('uname', '-a')


async def add_battery(telemetry: typing.Dict[str, typing.Any]) -> None:
    info = psutil.sensors_battery()
    if not info:
        return
    telemetry['battery'] = {
        'percent': info.percent,
        'ac_power': info.power_plugged,
    }


async def add_temperature_sensors(telemetry: typing.Dict[str, typing.Any]) -> None:
    for source, sensors in psutil.sensors_temperatures().items():
        if source == 'coretemp':
            # Intel
            telemetry['cpu_temperature'] = sensors[0].current
        elif source == 'k10temp':
            # AMD
            package_temp = None
            for s in sensors:
                if s.label == 'Tdie':
                    package_temp = s.current
                    break
                elif s.label == 'Tctl':
                    package_temp = s.current
            if package_temp:
                telemetry['cpu_temperature'] = package_temp
        elif source == 'cpu_thermal':
            # Raspberry Pi
            telemetry['cpu_temperature'] = sensors[0].current

        data = []
        for s in sensors:
            add = {
                'temperature': s.current,
            }
            if s.label and len(s.label) > 0:
                add['label'] = s.label
            data.append(add)
        if len(data) == 0:
            continue
        if 'sensors' not in telemetry:
            telemetry['sensors'] = {}
        telemetry['sensors'][source] = data
