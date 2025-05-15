import typing
import re
import os
from tempfile import mkstemp
from forge.timeparse import parse_iso8601_duration


_TIMELIKE_INTERVAL = re.compile(
    r'(?:(\d+(?:\.\d*)?):)?'
    r'(?:(\d+(?:\.\d*)?):)?'
    r'(\d+(?:\.\d*)?):'
    r'(\d+(?:\.\d*)?)',
    flags=re.IGNORECASE
)


def parse_interval(interval: typing.Optional[typing.Union[str, float, int, dict, bool]],
                   default: float = None) -> float:
    if interval is None:
        if default is None:
            raise ValueError("required interval missing")
        return default

    if isinstance(interval, dict):
        days = float(interval.get("DAYS", interval.get("DAY", 0)))
        hours = float(interval.get("HOURS", interval.get("HOUR", 0)))
        minutes = float(interval.get("MINUTES", interval.get("MIN", 0)))
        seconds = float(interval.get("SECONDS", interval.get("SEC", 0)))
        return days * 24 * 60 * 60 + hours * 60 * 60 + minutes * 60 + seconds

    if isinstance(interval, bool):
        if interval:
            if default is None:
                raise ValueError("required interval missing")
            return default
        return 0

    if not isinstance(interval, str):
        try:
            return float(interval)
        except (TypeError, ValueError):
            pass

    m = _TIMELIKE_INTERVAL.fullmatch(interval)
    if m:
        return (
            float(m.group(1) or 0) * 24 * 60 * 60 +
            float(m.group(2) or 0) * 60 * 60 +
            float(m.group(3) or 0) * 60 +
            float(m.group(4) or 0)
        )

    return parse_iso8601_duration(interval)


def write_replace_file(target_file: str, working_directory: str, write_file: typing.Callable[[str], None]) -> None:
    next_file: typing.Optional[str] = None
    next_fd: typing.Optional[int] = None
    try:
        next_fd, next_file = mkstemp(dir=working_directory)
        write_file(next_file)

        # mkstemp hard codes the mode to 0o600, so manually apply the umask
        try:
            umask = os.umask(0o666)
            os.umask(umask)
            umask |= 0o111
        except NotImplementedError:
            umask = 0o033
        file_mode = 0o666 & ~umask
        os.chmod(next_file, file_mode)

        os.replace(next_file, target_file)
        next_file = None
    finally:
        if next_fd is not None:
            os.close(next_fd)
        if next_file is not None:
            try:
                os.unlink(next_file)
            except OSError:
                pass

