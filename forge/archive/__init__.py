import typing
from dynaconf import Dynaconf
from dynaconf.constants import DEFAULT_SETTINGS_FILES

CONFIGURATION = Dynaconf(
    environments=False,
    lowercase_read=False,
    merge_enabled=True,
    default_settings_paths=DEFAULT_SETTINGS_FILES,
)

DEFAULT_ARCHIVE_TCP_PORT = 9123
