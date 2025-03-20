import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("2009-01-01").timestamp()
DATA_END_TIME: float = parse_iso8601_time("2025-03-20T19:00:00Z").timestamp()