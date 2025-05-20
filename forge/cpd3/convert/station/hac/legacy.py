import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("2018-07-05").timestamp()
DATA_END_TIME: float = parse_iso8601_time("2025-05-29T12:59:00Z").timestamp()