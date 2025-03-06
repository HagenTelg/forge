import typing
import time
from forge.timeparse import parse_iso8601_time


DATA_START_TIME: float = parse_iso8601_time("2005-05-05").timestamp()
DATA_END_TIME: float = parse_iso8601_time("2020-09-02").timestamp()