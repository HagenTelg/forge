import typing
import asyncio
from .command import command_output


async def add_ntp_info(telemetry: typing.Dict[str, typing.Any]) -> None:
    data = {}

    try:
        data['chrony_status'] = await command_output('chronyc', '-n', 'tracking', silent=True, check_exit=True)
        data['chrony_sources'] = await command_output('chronyc', '-n', 'sources', silent=True, check_exit=True)
        data['chrony_sourcestats'] = await command_output('chronyc', '-n', 'sourcestats', silent=True, check_exit=True)
    except (OSError, FileNotFoundError):
        pass

    try:
        data['timedatectl_status'] = await command_output('timedatectl', 'status', silent=True, check_exit=True)
    except (OSError, FileNotFoundError):
        pass

    try:
        data['ntpd_peers'] = await command_output('ntpq', '-n', '-c', 'peers', silent=True, check_exit=True)
        data['ntpd_vars'] = await command_output('ntpq', '-n', '-c', 'readvar', silent=True, check_exit=True)
    except (OSError, FileNotFoundError):
        pass

    if len(data) > 0:
        telemetry['ntp'] = data
