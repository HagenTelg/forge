import typing
import datetime
import time
from math import isfinite
from .base import CommunicationsError


def parse_number(value: bytes) -> float:
    try:
        v = float(value.strip())
    except (ValueError, OverflowError):
        raise CommunicationsError(f"invalid number {value}")
    if not isfinite(v):
        raise CommunicationsError("converted number is not finite")
    return v


def parse_date(raw: bytes,
               date_separator: bytes = b'-',
               two_digit_year: typing.Optional[bool] = None) -> datetime.date:
    try:
        fields = raw.split(date_separator)
        if len(fields) != 3:
            raise CommunicationsError("invalid number of date fields")

        year = int(fields[0].strip())
        if two_digit_year and (year < 0 or year > 99):
            raise CommunicationsError(f"invalid year {year}")
        if two_digit_year or (two_digit_year is None and (0 <= year <= 99)):
            td = time.gmtime()
            current_century = td.tm_year - (td.tm_year % 100)
            year += current_century
            if year > td.tm_year + 50:
                year -= 100
        if year < 1900 or year > 2999:
            raise CommunicationsError(f"invalid year {year}")

        month = int(fields[1].strip())
        day = int(fields[2].strip())
        return datetime.date(year, month, day)
    except ValueError as e:
        raise CommunicationsError(e)


def parse_date_fixed(raw: bytes, year_digits: int = 4, month_digits: int = 2, day_digits: int = 2) -> datetime.date:
    try:
        if len(raw) != year_digits + month_digits + day_digits:
            raise CommunicationsError("invalid date length")

        year = int(raw[:year_digits].strip())
        if year < 1900 or year > 2999:
            raise CommunicationsError(f"invalid year {year}")
        month = int(raw[year_digits:(year_digits+month_digits)].strip())
        day = int(raw[(year_digits+month_digits):].strip())
        return datetime.date(year, month, day)
    except ValueError as e:
        raise CommunicationsError(e)


def parse_time(raw: bytes,
               time_separator: bytes = b':') -> datetime.time:
    try:
        fields = raw.split(time_separator)
        if len(fields) != 3:
            raise CommunicationsError("invalid number of time fields")
        hour = int(fields[0].strip())
        minute = int(fields[1].strip())
        second = int(fields[2].strip())
        return datetime.time(hour, minute, second, tzinfo=datetime.timezone.utc)
    except ValueError as e:
        raise CommunicationsError(e)


def parse_time_fixed(raw: bytes, hour_digits: int = 2, minute_digits: int = 2, second_digits: int = 2) -> datetime.time:
    try:
        if len(raw) != hour_digits + minute_digits + second_digits:
            raise CommunicationsError("invalid time length")
        hour = int(raw[:hour_digits].strip())
        minute = int(raw[hour_digits:(hour_digits+minute_digits)].strip())
        second = int(raw[(hour_digits+minute_digits):].strip())
        return datetime.time(hour, minute, second, tzinfo=datetime.timezone.utc)
    except ValueError as e:
        raise CommunicationsError(e)


def parse_date_and_time(date_field: bytes, time_field: bytes,
                        date_separator: bytes = b'-', two_digit_year: typing.Optional[bool] = None,
                        time_separator: bytes = b':') -> datetime.datetime:
    try:
        d = parse_date(date_field, date_separator=date_separator, two_digit_year=two_digit_year)
        t = parse_time(time_field, time_separator=time_separator)
        return datetime.datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, tzinfo=t.tzinfo)
    except ValueError as e:
        raise CommunicationsError(e)


def parse_date_and_time_fixed(
        date_field: bytes, time_field: bytes,
        year_digits: int = 4, month_digits: int = 2, day_digits: int = 2,
        hour_digits: int = 2, minute_digits: int = 2, second_digits: int = 2) -> datetime.datetime:
    try:
        d = parse_date_fixed(date_field, year_digits=year_digits, month_digits=month_digits, day_digits=day_digits)
        t = parse_time_fixed(time_field, hour_digits=hour_digits, minute_digits=minute_digits,
                             second_digits=second_digits)
        return datetime.datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, tzinfo=t.tzinfo)
    except ValueError as e:
        raise CommunicationsError(e)


def parse_datetime_field(dt: bytes, datetime_seperator: bytes = b' ', **kwargs) -> datetime.datetime:
    try:
        subfields = dt.split(datetime_seperator)
        if len(subfields) != 2:
            raise CommunicationsError("invalid number of datetime fields")
        return parse_date_and_time(subfields[0].strip(), subfields[1].strip(), **kwargs)
    except ValueError as e:
        raise CommunicationsError(e)


def parse_datetime_field_fixed(dt: bytes, datetime_seperator: bytes = b' ', **kwargs) -> datetime.datetime:
    try:
        if datetime_seperator:
            subfields = dt.split(datetime_seperator)
            if len(subfields) != 2:
                raise CommunicationsError("invalid number of datetime fields")
            return parse_date_and_time_fixed(subfields[0].strip(), subfields[1].strip(), **kwargs)
        length_date = kwargs.get('year_digits', 4) + kwargs.get('month_digits', 2) + kwargs.get('day_digits', 2)
        return parse_date_and_time_fixed(dt[:length_date], dt[length_date:], **kwargs)
    except ValueError as e:
        raise CommunicationsError(e)


def parse_flags_bits(field: bytes, dispatch: typing.Dict[int, typing.Callable[[bool], typing.Any]],
                     base: typing.Optional[int] = 16) -> None:
    try:
        if base:
            flags = int(field.strip(), base)
        else:
            flags = int(field.strip())
    except (ValueError, OverflowError):
        raise CommunicationsError(f"invalid flags {field}")
    if flags < 0:
        raise CommunicationsError(f"negative flags {field}")
    for bit, flag in dispatch.items():
        flag((flags & bit) != 0)
