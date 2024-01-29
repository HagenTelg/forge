import typing
import datetime
import time
from math import floor


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


def start_of_month(year: int, month: int) -> int:
    return int(floor(datetime.datetime(
        year, month, 1,
        tzinfo=datetime.timezone.utc
    ).timestamp()))


def start_of_month_ms(year: int, month: int) -> int:
    return int(floor(start_of_month(year, month) * 1000.0))


def end_of_month(year: int, month: int) -> int:
    month += 1
    if month > 12:
        month = 1
        year += 1
    return start_of_month(year, month)


def end_of_month_ms(year: int, month: int) -> int:
    return int(floor(end_of_month(year, month) * 1000.0))


def month_bounds(year: int, month: int) -> typing.Tuple[int, int]:
    return start_of_month(year, month), end_of_month(year, month)


def month_bounds_ms(year: int, month: int) -> typing.Tuple[int, int]:
    start, end = month_bounds(year, month)
    return start * 1000, end * 1000


def months_since_epoch(epoch: float) -> int:
    ts = time.gmtime(epoch)
    return (ts.tm_year - 1970) * 12 + (ts.tm_mon - 1)


def year_month_from_epoch_month(epoch_months: int) -> typing.Tuple[int, int]:
    year = int(floor(epoch_months / 12)) + 1970
    month = int(epoch_months % 12) + 1
    return year, month


def start_of_epoch_month(epoch_months: int) -> int:
    return start_of_month(*year_month_from_epoch_month(epoch_months))


def start_of_epoch_month_ms(epoch_months: int) -> int:
    return start_of_epoch_month(epoch_months) * 1000


def containing_epoch_month_range(start: float, end: float) -> typing.Tuple[int, int]:
    start_month_number = months_since_epoch(start)
    end_month_number = months_since_epoch(end)
    month_end = start_of_epoch_month(end_month_number)
    if month_end < end:
        end_month_number += 1
    return start_month_number, end_month_number


def round_to_month(start: float, end: float) -> typing.Tuple[int, int]:
    start_month_number, end_month_number = containing_epoch_month_range(start, end)
    return start_of_epoch_month(start_month_number), start_of_epoch_month(end_month_number)

