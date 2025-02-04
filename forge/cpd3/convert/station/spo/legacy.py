import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("1974-02-09").timestamp()
# DATA_END_TIME: float = parse_iso8601_time("2025-02-19T15:00:00Z").timestamp()
