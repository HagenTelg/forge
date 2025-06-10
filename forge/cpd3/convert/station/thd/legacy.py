import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("2002-04-01").timestamp()  # Ozone data
DATA_END_TIME: float = parse_iso8601_time("2025-05-22T18:45:00").timestamp()