import typing
import time
from forge.timeparse import parse_iso8601_time

# Used for sync start time
DATA_START_TIME: float = parse_iso8601_time("2011-01-01").timestamp()