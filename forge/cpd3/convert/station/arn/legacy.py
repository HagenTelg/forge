import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("2005-12-31").timestamp()
DATA_END_TIME: float = parse_iso8601_time("2025-06-12T18:00:00Z").timestamp()