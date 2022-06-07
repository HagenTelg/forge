import typing
import sys
import time
from netCDF4 import Dataset
from forge.const import __version__
from forge.formattime import format_iso8601_time


def append_history(target: Dataset, component: str, now: float = None) -> None:
    timestamp = format_iso8601_time(now or time.time())

    history_line = f"{timestamp},{component},{__version__},{' '.join(sys.argv)}"

    history_data = getattr(target, 'history', None)
    if history_data:
        history_data = history_data + "\n" + history_line
    else:
        history_data = history_line

    target.history = history_data


