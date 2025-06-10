import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("2011-06-09").timestamp()
DATA_END_TIME: float = parse_iso8601_time("2025-05-05T16:00:00").timestamp()