import typing
import re
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
