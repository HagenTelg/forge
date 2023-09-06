import typing
from forge.vis.access import AccessUser


def dashboard_accessible(user: AccessUser) -> bool:
    return user.allow_global('dashboard')


def is_available(user: AccessUser, station: typing.Optional[str], entry_code: str) -> bool:
    if entry_code.startswith("example-"):
        return True
    return user.allow_mode(station, entry_code)

