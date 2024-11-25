import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("1976-05-06").timestamp()
DATA_END_TIME: float = parse_iso8601_time("2024-11-04T18:00:00Z").timestamp()
