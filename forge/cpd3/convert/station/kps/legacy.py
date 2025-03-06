import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("2006-05-09").timestamp()
DATA_END_TIME: float = parse_iso8601_time("2022-03-04").timestamp()