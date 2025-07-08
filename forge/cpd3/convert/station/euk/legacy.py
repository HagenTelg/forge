import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("2016-07-21").timestamp()
DATA_END_TIME: float = parse_iso8601_time("2025-07-08T13:00:00Z").timestamp()