import typing
import re
import datetime


MATCH_ISO8601_DURATION = re.compile(
    r'P?(?:(?:(\d+)D)?T?)?'
    r'(?:(\d+)H)?'
    r'(?:(\d+)M)?'
    r'(?:(\d+(?:\.\d*)?)S?)?',
    flags=re.IGNORECASE
)

MATCH_ISO8601_TIME = re.compile(
    r'(\d{4})-?(\d{2})-?(\d{2})'
    r'T?'
    r'(?:(\d{2}):?(\d{2}):?(\d{2}(\.\d*)?))?'
    r'Z?',
    flags=re.IGNORECASE
)


def parse_iso8601_duration(s: str) -> float:
    m = MATCH_ISO8601_DURATION.fullmatch(s)
    if m:
        return (
                float(m.group(1) or 0) * 24 * 60 * 60 +
                float(m.group(2) or 0) * 60 * 60 +
                float(m.group(3) or 0) * 60 +
                float(m.group(4) or 0)
        )

    raise ValueError("invalid interval format")


def parse_iso8601_time(s: str) -> datetime.datetime:
    m = MATCH_ISO8601_TIME.fullmatch(s)
    if m:
        microseconds = 0
        if m.group(7):
            fractional = "0." + m.group(7)
            microseconds = round(float(fractional) * 1e6)
        return datetime.datetime(
            int(m.group(1)), int(m.group(2)), int(m.group(3)),
            int(m.group(4) or 0), int(m.group(5) or 0), int(m.group(6) or 0),
            microsecond=microseconds,
            tzinfo=datetime.timezone.utc
        )

    raise ValueError("invalid time format")
