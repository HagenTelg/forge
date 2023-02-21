import typing
import re
import datetime
from base64 import urlsafe_b64encode
from forge.crypto import PublicKey, key_to_bytes
from .storage.protocol import FileType

_time_replace = re.compile(r"\{time:([^{}]+)}")


def completion_directory(base: str, key: PublicKey, station: str, file_type: typing.Union[str, FileType]) -> str:
    if '{key}' in base:
        base = base.replace('{key}', urlsafe_b64encode(key_to_bytes(key)).decode('ascii'))
    if '{type}' in base:
        if isinstance(file_type, FileType):
            file_type = file_type.name
        base = base.replace('{type}', file_type.lower())
    if '{station}' in base:
        base = base.replace('{station}', station.lower())
    if '{time' in base:
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        base = _time_replace.sub(lambda m: now.strftime(m.group(1)), base)
    return base
