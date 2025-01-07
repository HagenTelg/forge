import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("1976-05-06").timestamp()
DATA_END_TIME: float = parse_iso8601_time("2025-01-07T04:00:00Z").timestamp()
