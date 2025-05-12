import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("2008-10-10").timestamp()
DATA_END_TIME: float = parse_iso8601_time("2025-06-27T13:00:00").timestamp()