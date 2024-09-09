import typing
import logging
from math import ceil
from forge.timeparse import parse_iso8601_time

_LOGGER = logging.getLogger(__name__)


def parse_history(value: typing.Optional[str],
                  limit_start: typing.Optional[int] = None,
                  limit_end: typing.Optional[int] = None) -> typing.Dict[int, str]:
    result: typing.Dict[int, str] = dict()
    if not value:
        return result

    for line in str(value).strip().split('\n'):
        try:
            end_time, contents = line.split(',', 1)
        except ValueError:
            _LOGGER.warning(f"Malformed history line {line}")
            continue
        end_time = int(ceil(parse_iso8601_time(str(end_time)).timestamp() * 1000.0))
        if limit_start and end_time < limit_start:
            continue
        if limit_end and end_time >= limit_end:
            continue
        result[end_time] = contents
    return result

