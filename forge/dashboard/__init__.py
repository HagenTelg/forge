import re
from enum import Enum
from dynaconf import Dynaconf
from dynaconf.constants import DEFAULT_SETTINGS_FILES

CONFIGURATION = Dynaconf(
    environments=False,
    lowercase_read=False,
    merge_enabled=True,
    default_settings_paths=DEFAULT_SETTINGS_FILES,
)


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"

    @property
    def abnormal(self) -> bool:
        return self == Severity.WARNING or self == Severity.ERROR


_MATCH_STATION = re.compile(br'[A-Za-z][0-9A-Za-z_]{0,31}')
_MATCH_CODE = re.compile(br'[A-Za-z][0-9A-Za-z_-]{0,63}')


def is_valid_station(station: str) -> bool:
    if not station:
        return False
    try:
        encoded = station.encode('ascii')
    except UnicodeEncodeError:
        return False
    return _MATCH_STATION.fullmatch(encoded) is not None


def is_valid_code(code: str) -> bool:
    if not code:
        return False
    try:
        encoded = code.encode('ascii')
    except UnicodeEncodeError:
        return False
    return _MATCH_CODE.fullmatch(encoded) is not None