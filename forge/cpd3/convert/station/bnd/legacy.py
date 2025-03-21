import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("1994-07-14").timestamp()
DATA_END_TIME: float = parse_iso8601_time("2025-03-25T14:00:00Z").timestamp()
