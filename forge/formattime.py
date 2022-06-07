import typing
import time


def format_iso8601_duration(duration: float, milliseconds: bool = False) -> str:
    content = ""

    if not milliseconds:
        duration = round(duration)
        seconds = int(duration % 60)
        if seconds:
            content = f"{seconds}S" + content
        duration -= seconds
    else:
        seconds = duration % 60
        milliseconds = int(seconds * 1000) % 1000
        seconds = int(seconds)
        duration = int(duration)
        if seconds or milliseconds:
            content = f"{seconds}.{milliseconds:03}S" + content
        duration -= seconds

    minutes = int((duration / 60) % 60)
    if minutes:
        content = f"{minutes}M" + content
    duration -= minutes * 60

    hours = int((duration / (60 * 60)) % 24)
    if hours:
        content = f"{hours}H" + content
    duration -= hours * 60 * 60

    days = int(duration / (24 * 60 * 60))
    if days:
        if content:
            return f"P{days}DT" + content
        else:
            return f"P{days}D"
    else:
        return "PT" + content


def _date(ts, delimiter: str = '-') -> str:
    return f"{ts.tm_year:04}{delimiter}{ts.tm_mon:02}{delimiter}{ts.tm_mday:02}"


def _time_of_day(ts, epoch_ms: typing.Optional[int] = None, delimiter: str = ':') -> str:
    seconds = f"{ts.tm_sec:02}"
    if epoch_ms is not None:
        seconds = seconds + f".{epoch_ms:03}"

    return f"{ts.tm_hour:02}{delimiter}{ts.tm_min:02}{delimiter}{seconds}"


def format_time_of_day(t: float, delimited: bool = True, milliseconds: bool = False):
    if milliseconds:
        epoch_ms = round(t * 1000.0)
        ts = time.gmtime(int(epoch_ms / 1000))
        epoch_ms %= 1000
    else:
        ts = time.gmtime(t)
        epoch_ms = None

    return _time_of_day(ts, epoch_ms=epoch_ms, delimiter=(":" if delimited else ""))


def format_iso8601_time(t: float, delimited: bool = True, milliseconds: bool = False) -> str:
    if milliseconds:
        epoch_ms = round(t * 1000.0)
        ts = time.gmtime(int(epoch_ms / 1000))
        epoch_ms %= 1000
    else:
        ts = time.gmtime(t)
        epoch_ms = None

    date_str = _date(ts, delimiter=("-" if delimited else ""))
    time_str = _time_of_day(ts, epoch_ms=epoch_ms, delimiter=(":" if delimited else ""))
    return date_str + "T" + time_str + "Z"


def format_export_time(t: float) -> str:
    ts = time.gmtime(t)
    return _date(ts) + " " + _time_of_day(ts)
