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
    if year_end < end or start_year_number == end_year_number:
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
    if month_end < end or start_month_number == end_month_number:
        end_month_number += 1
    return start_month_number, end_month_number


def round_to_month(start: float, end: float) -> typing.Tuple[int, int]:
    start_month_number, end_month_number = containing_epoch_month_range(start, end)
    return start_of_epoch_month(start_month_number), start_of_epoch_month(end_month_number)


def _day_of_week(year: int, month: int, day: int) -> int:
    dt = datetime.date(year, month, day)
    return (dt.weekday() + 1) % 7


def julian_day(year: int, month: int, day: int) -> int:
    if month > 2:
        month -= 3
    else:
        year -= 1
        month += 9
    c = int(floor(year / 100))
    ya = year - 100 * c
    return int(floor((146097 * c) / 4) + floor((1461 * ya) / 4) + floor((153 * month + 2) / 5) + day + 1721119)


def start_of_week(year: int, week: int) -> int:
    _week_offset = (0, -1, -2, -3, 3, 2, 1)

    dow = (_day_of_week(year, 1, 1) + 6) % 7
    base_jd = julian_day(year, 1, 1)
    jd = base_jd + _week_offset[dow] + (week - 1) * 7
    if jd - base_jd < 0:
        year -= 1
    count = 1
    while (jd - julian_day(year + count, 1, 1)) > 0:
        count += 1
    year += count - 1
    return start_of_year(year) + (jd - julian_day(year, 1, 1)) * 24 * 60 * 60


def end_of_week(year: int, week: int) -> int:
    return start_of_week(year, week) + 7 * 24 * 60 * 60


def start_of_quarter(year: int, quarter: int) -> int:
    _quarter_start_doy = (1, 91, 182, 274)
    return start_of_year(year) + (_quarter_start_doy[quarter-1] - 1) * 24 * 60 * 60


def end_of_quarter(year: int, quarter: int) -> int:
    _quarter_start_doy = (1, 91, 182, 274)
    if quarter == 4:
        return start_of_year(year + 1)
    return start_of_year(year) + (_quarter_start_doy[quarter] - 1) * 24 * 60 * 60
