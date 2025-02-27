import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("2013-04-12").timestamp()
DATA_END_TIME: float = parse_iso8601_time("2017-06-07").timestamp()