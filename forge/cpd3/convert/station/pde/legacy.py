import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("2018-04-18").timestamp()
DATA_END_TIME: float = parse_iso8601_time("2021-08-01").timestamp()