import typing
import asyncio
import psutil
import re
from dynaconf import Dynaconf
from dynaconf.constants import DEFAULT_SETTINGS_FILES


_MATCH_TTY = re.compile(r'^tty\d+$')


def convert_login_user(users: typing.List[typing.Dict[str, typing.Optional[str]]]) -> typing.Optional[str]:
    for user in users:
        if 'name' not in user:
            continue
        if user['name'] == 'root':
            continue
        if not _MATCH_TTY.match(user.get('terminal', '')):
            continue
        if 'host' not in user:
            continue
        if user['host'] != 'localhost' and user['host'] != user.get('terminal'):
            continue
        return str(user['name'])
    return None


async def add_login_user(telemetry: typing.Dict[str, typing.Any]) -> None:
    configuration = Dynaconf(
        environments=False,
        lowercase_read=False,
        merge_enabled=True,
        default_settings_paths=DEFAULT_SETTINGS_FILES,
    )
    configured_user = configuration.get('TELEMETRY.LOGIN_USER')
    if configured_user:
        telemetry['login_user'] = str(configured_user)
        return

    for user in psutil.users():
        if user.name == 'root':
            continue
        if user.terminal is None or not _MATCH_TTY.match(user.terminal):
            continue
        if user.host is None:
            continue
        if user.host != 'localhost' and user.host != user.terminal:
            continue
        telemetry['login_user'] = user.name
        return
