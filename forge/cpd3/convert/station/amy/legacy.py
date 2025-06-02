import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("2018-01-03").timestamp()
DATA_END_TIME: float = parse_iso8601_time("2024-10-12").timestamp()