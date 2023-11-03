import typing
import datetime
import time
from math import floor, ceil


def start_of_year(year: int) -> int:
    return int(floor(datetime.datetime(
        year, 1, 1,
        tzinfo=datetime.timezone.utc
    ).timestamp()))


def start_of_year_ms(year: int) -> int:
    return int(floor(start_of_year(year) * 1000.0))


def end_of_year(year: int) -> int:
    return int(floor(datetime.datetime(
        year + 1, 1, 1,
        tzinfo=datetime.timezone.utc
    ).timestamp()))


def end_of_year_ms(year: int) -> int:
    return int(end_of_year(year) * 1000)


def year_bounds(year: int) -> typing.Tuple[int, int]:
    return start_of_year(year), end_of_year(year)


def year_bounds_ms(year: int) -> typing.Tuple[int, int]:
    start, end = year_bounds(year)
    return start * 1000, end * 1000


def containing_year_range(start: float, end: float) -> typing.Tuple[int, int]:
    start_year_number = time.gmtime(start).tm_year
    end_year_number = time.gmtime(end).tm_year
    year_end = start_of_year(end_year_number)
    if year_end < end:
        end_year_number += 1
    return start_year_number, end_year_number


def round_to_year(start: float, end: float) -> typing.Tuple[int, int]:
    start_year_number, end_year_number = containing_year_range(start, end)
    return start_of_year(start_year_number), start_of_year(end_year_number)
