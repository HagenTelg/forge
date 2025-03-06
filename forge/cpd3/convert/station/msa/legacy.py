import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("2013-01-01").timestamp()
DATA_END_TIME: float = parse_iso8601_time("2022-06-09").timestamp()