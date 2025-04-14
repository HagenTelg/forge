import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("1975-12-01").timestamp()  # Ozone data
DATA_END_TIME: float = parse_iso8601_time("2025-04-14T17:00:00Z").timestamp()
