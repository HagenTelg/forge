import typing
import re
import datetime
import forge.logicaltime as lt


_MATCH_ISO8601_DURATION = re.compile(
    r'P?(?:(?:(\d+)D)?T?)?'
    r'(?:(\d+)H)?'
    r'(?:(\d+)M)?'
    r'(?:(\d+(?:\.\d*)?)S?)?',
    flags=re.IGNORECASE
)
_AMBIGUOUS_ISO8601_DURATION = re.compile(
    r'P(?:\d+)M',
    flags=re.IGNORECASE
)
_MATCH_ISO8601_TIME = re.compile(
    r'(\d{4})-?(\d{2})-?(\d{2})'
    r'(?:T|\s+)?'
    r'(?:(\d{2}):?(\d{2}):?(\d{2}(\.\d*)?))?'
    r'Z?',
    flags=re.IGNORECASE
)
_MATCH_INF = re.compile(
    r'^\s*(?:undef|inf|none|∞)',
    flags=re.IGNORECASE
)
_MATCH_WHOLE_WEEK = re.compile(
    r'\s*(?:w|week)\s*',
    flags=re.IGNORECASE
)
_MATCH_WEEK = re.compile(
    r'\s*(\d{4})?w(\d{1,2})\s*',
    flags=re.IGNORECASE
)
_MATCH_QUARTER = re.compile(
    r'\s*(\d{4})?Q([1234])\s*',
    flags=re.IGNORECASE
)
_MATCH_FRACTIONAL_YEAR = re.compile(
    r'\s*(\d{4})\.(\d+)\s*',
    flags=re.IGNORECASE
)
_MATCH_YEAR_DOY = re.compile(
    r'\s*(\d{4}):(\d{1,3}(\.\d*)?)\s*',
    flags=re.IGNORECASE
)
_MATCH_EPOCH = re.compile(
    r'\s*E(M?)\s*:\s*(\d+)\s*',
    flags=re.IGNORECASE
)
_TIME_PART_SPLIT = re.compile(
    r'\s+|[:TZ-]',
    flags=re.IGNORECASE
)


def parse_iso8601_duration(s: str) -> float:
    m = _MATCH_ISO8601_DURATION.fullmatch(s)
    if m:
        # "P1M" should be one month, but we're only parsing fixed intervals here, and we want to
        # accept things like P1H for ease of use.  One minute should be specified as either "1M" or "PT1M".
        if _AMBIGUOUS_ISO8601_DURATION.fullmatch(s):
            raise ValueError("ambiguous interval format")
        return (
                float(m.group(1) or 0) * 24 * 60 * 60 +
                float(m.group(2) or 0) * 60 * 60 +
                float(m.group(3) or 0) * 60 +
                float(m.group(4) or 0)
        )

    raise ValueError("invalid interval format")


def parse_iso8601_time(s: str) -> datetime.datetime:
    m = _MATCH_ISO8601_TIME.fullmatch(s)
    if m:
        microseconds = 0
        if m.group(7):
            fractional = "0." + m.group(7)
            microseconds = round(float(fractional) * 1e6)
        return datetime.datetime(
            int(m.group(1)), int(m.group(2)), int(m.group(3)),
            int(m.group(4) or 0), int(m.group(5) or 0), int(m.group(6) or 0),
            microsecond=microseconds,
            tzinfo=datetime.timezone.utc
        )

    raise ValueError("invalid time format")


def _parse_unambiguous_absolute(
        s: str,
        year: typing.Optional[int] = None,
) -> typing.Tuple[datetime.datetime, datetime.datetime]:
    m = _MATCH_WEEK.fullmatch(s)
    if m:
        week_year = m.group(1)
        if week_year:
            week_year = int(week_year)
        else:
            week_year = year
        week = int(m.group(2))
        if week_year and 1970 <= week_year <= 2999 and 1 <= week <= 53:
            start = datetime.datetime.fromtimestamp(lt.start_of_week(week_year, week), tz=datetime.timezone.utc)
            end = datetime.datetime.fromtimestamp(lt.end_of_week(week_year, week), tz=datetime.timezone.utc)
            return start, end

    m = _MATCH_QUARTER.fullmatch(s)
    if m:
        quarter_year = m.group(1)
        if quarter_year:
            quarter_year = int(quarter_year)
        else:
            quarter_year = year
        quarter = int(m.group(2))
        if quarter_year and 1970 <= quarter_year <= 2999 and 1 <= quarter <= 4:
            start = datetime.datetime.fromtimestamp(lt.start_of_quarter(quarter_year, quarter), tz=datetime.timezone.utc)
            end = datetime.datetime.fromtimestamp(lt.end_of_quarter(quarter_year, quarter), tz=datetime.timezone.utc)
            return start, end

    m = _MATCH_FRACTIONAL_YEAR.fullmatch(s)
    if m:
        fractional_year = int(m.group(1))
        fraction = float("0." + m.group(2))
        if 1970 <= fractional_year <= 2999 and 0.0 <= fraction <= 1.0:
            start, end = lt.year_bounds(fractional_year)
            t = start + (end - start) * fraction
            start = datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc)
            return start, start

    m = _MATCH_YEAR_DOY.fullmatch(s)
    if m:
        doy_year = int(m.group(1))
        doy = float(m.group(2))
        doy_decimals = m.group(3)
        if 1970 <= doy_year <= 2999 and 1.0 <= doy <= 366.0:
            start = lt.start_of_year(doy_year)
            if doy_decimals and len(doy_decimals) == 3:
                # 200.13 rounds to an exact hour
                doy = round((doy - 1) * 24) * 60 * 60
                end = 60 * 60
            else:
                doy = round((doy - 1) * (24 * 60)) * 60
                end = 0
            t = start + doy
            start = datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc)
            end = start + datetime.timedelta(seconds=end)
            return start, end

    m = _MATCH_ISO8601_TIME.fullmatch(s)
    if m:
        microseconds = 0
        if m.group(7):
            fractional = "0." + m.group(7)
            microseconds = round(float(fractional) * 1e6)
        start = datetime.datetime(
            int(m.group(1)), int(m.group(2)), int(m.group(3)),
            int(m.group(4) or 0), int(m.group(5) or 0), int(m.group(6) or 0),
            microsecond=microseconds,
            tzinfo=datetime.timezone.utc
        )
        end = start
        if not m.group(7) and not m.group(6):
            if m.group(5):
                end += datetime.timedelta(minutes=1)
            elif m.group(4):
                end += datetime.timedelta(hours=1)
            elif m.group(3):
                end += datetime.timedelta(days=1)
        return start, end

    m = _MATCH_EPOCH.fullmatch(s)
    if m:
        epoch = int(m.group(2))
        if m.group(1) or epoch > 31536000000:
            epoch /= 1000.0
        start = datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc)
        return start, start

    raise ValueError("invalid time format")


def _parse_any_offset(s: str) -> float:
    s = s.strip()

    if _MATCH_WHOLE_WEEK.fullmatch(s):
        return 7 * 24 * 60 * 60

    if s.startswith('P'):
        return parse_iso8601_duration(s)

    def valid_number(n: str) -> float:
        n = int(n)
        if n <= 0:
            raise ValueError
        return n

    if s.endswith('s'):
        try:
            return valid_number(s[:-1])
        except ValueError:
            pass
    elif s.endswith('m'):
        try:
            return valid_number(s[:-1]) * 60
        except ValueError:
            pass
    elif s.endswith('h'):
        try:
            return valid_number(s[:-1]) * 60 * 60
        except ValueError:
            pass
    elif s.endswith('d'):
        try:
            return valid_number(s[:-1]) * 24 * 60 * 60
        except ValueError:
            pass
    elif s.endswith('w'):
        try:
            return valid_number(s[:-1]) * 7 * 24 * 60 * 60
        except ValueError:
            pass

    raise ValueError("invalid offset format")


def _apply_offset(reference: datetime.datetime, offset: float) -> datetime.datetime:
    t = reference.timestamp()
    t += offset
    return datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc)


def _parse_any_single_time(parts: typing.List[str],
                           reference: typing.Optional[datetime.datetime] = None,
                           is_end: bool = False) -> datetime.datetime:
    def doy_to_seconds(doy: float, raw: str) -> float:
        fraction_parts = raw.split('.', 1)
        if len(fraction_parts) == 2 and len(fraction_parts[1]) == 2:
            # 200.13 rounds to an exact hour
            return round((doy - 1) * 24) * 60 * 60
        return round((doy - 1) * (24 * 60)) * 60

    if len(parts) == 1 and reference:
        try:
            return _apply_offset(reference, _parse_any_offset(parts[0]) * (1 if is_end else -1))
        except ValueError:
            pass

        try:
            doy = float(parts[0])
            if 1.0 <= doy <= 366.0:
                doy = doy_to_seconds(doy, parts[0])
                doy = datetime.datetime(reference.year, 1, 1, tzinfo=datetime.timezone.utc).timestamp() + doy
                doy = datetime.datetime.fromtimestamp(doy, tz=datetime.timezone.utc)
                if is_end and doy > reference:
                    return doy
                elif not is_end and doy < reference:
                    return doy
        except ValueError:
            pass

    if len(parts) == 1:
        try:
            year = int(parts[0])
            if 1970 <= year <= 2999:
                if is_end:
                    year += 1
                year = datetime.datetime(year, 1, 1, tzinfo=datetime.timezone.utc)
                if reference:
                    if is_end and year > reference:
                        return year
                    elif not is_end and year < reference:
                        return year
                else:
                    return year
        except ValueError:
            pass

    if len(parts) == 2:
        try:
            year = int(parts[0])
            doy = float(parts[1])
            if 1970 <= year <= 2999 and 1.0 <= doy <= 366.0:
                if is_end and int(doy) == doy:
                    doy += 1
                doy = doy_to_seconds(doy, parts[1])
                doy = lt.start_of_year(year) + doy
                doy = datetime.datetime.fromtimestamp(doy, tz=datetime.timezone.utc)
                if reference:
                    if is_end and doy > reference:
                        return doy
                    elif not is_end and doy < reference:
                        return doy
                else:
                    return doy
        except ValueError:
            pass

    if 3 <= len(parts) <= 6:
        try:
            year = int(parts[0])
            month = int(parts[1])
            day = int(parts[2])

            if len(parts) > 3:
                hour = int(parts[3])
            else:
                hour = 0
            if len(parts) > 4:
                minute = int(parts[4])
            else:
                minute = 0
            if len(parts) > 5:
                try:
                    second = int(parts[5])
                    microseconds = 0
                except ValueError:
                    raw_seconds = float(parts[5])
                    second = int(raw_seconds)
                    microseconds = int((raw_seconds - second) * 1E6)
            else:
                second = 0
                microseconds = 0

            if 1970 <= year <= 2999 and 1 <= month <= 12 and 1 <= day <= 31 and 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 60:
                dt = datetime.datetime(
                    year, month, day,
                    hour, minute, second,
                    microsecond=microseconds,
                    tzinfo=datetime.timezone.utc
                )
                if is_end:
                    if len(parts) == 3:
                        dt += datetime.timedelta(hours=24)
                    elif len(parts) == 4:
                        dt += datetime.timedelta(hours=1)
                    elif len(parts) == 5:
                        dt += datetime.timedelta(minutes=1)
                if reference:
                    if is_end and dt > reference:
                        return dt
                    elif not is_end and dt < reference:
                        return dt
                else:
                    return dt
        except ValueError:
            pass

    raise ValueError("invalid time format")


def parse_time_argument(s: str) -> datetime.datetime:
    try:
        t, _ = _parse_unambiguous_absolute(s)
        return t
    except ValueError:
        pass

    parts = _TIME_PART_SPLIT.split(s.strip())
    while parts and not parts[0]:
        parts = parts[1:]
    while parts and not parts[-1]:
        parts = parts[:-1]

    return _parse_any_single_time(parts)


def parse_time_bounds_arguments(args: typing.List[str]) -> typing.Tuple[datetime.datetime, datetime.datetime]:
    if len(args) == 0:
        raise ValueError("invalid time format")
    if len(args) == 1 and args[0].lower() == 'forever':
        return (
            datetime.datetime(1970, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc),
            datetime.datetime.now(tz=datetime.timezone.utc)
        )

    start: typing.Optional[datetime.datetime] = None
    end: typing.Optional[datetime.datetime] = None

    remaining = args
    if remaining[-1].lower() == 'now' or _MATCH_INF.match(remaining[-1]):
        end = datetime.datetime.now(tz=datetime.timezone.utc)
        remaining = remaining[:-1]
    if remaining and _MATCH_INF.match(remaining[0]):
        start = datetime.datetime(1970, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc)
        remaining = remaining[1:]

    def _fragment_remaining() -> typing.List[str]:
        result: typing.List[str] = list()
        for a in remaining:
            result.extend(_TIME_PART_SPLIT.split(a.strip()))
        while result and not result[0]:
            result = result[1:]
        while result and not result[-1]:
            result = result[:-1]
        return result

    absolute_offset_origin = None
    if not start and not end:
        if not remaining:
            raise ValueError("start and end time invalid")

        if len(remaining) == 1:
            try:
                return _parse_unambiguous_absolute(remaining[0])
            except ValueError:
                pass

        try:
            start, _ = _parse_unambiguous_absolute(remaining[0])
            remaining = remaining[1:]
        except ValueError:
            pass
        if not start:
            try:
                absolute_offset_origin, end = _parse_unambiguous_absolute(remaining[-1])
                remaining = remaining[:-1]
            except ValueError:
                pass

        if not start and not end:
            try:
                start_offset = _parse_any_offset(remaining[0])
                remaining = remaining[1:]
                end_offset = None
            except ValueError:
                start_offset = None
                try:
                    end_offset = _parse_any_offset(remaining[-1])
                    remaining = remaining[:-1]
                except ValueError:
                    end_offset = None

            if start_offset is not None:
                end = _parse_any_single_time(_fragment_remaining())
                remaining.clear()
                start = _apply_offset(end, start_offset * -1)
            elif end_offset is not None:
                start = _parse_any_single_time(_fragment_remaining())
                remaining.clear()
                end = _apply_offset(start, end_offset * 1)
            else:
                parts = _fragment_remaining()
                try:
                    start = _parse_any_single_time(parts)
                    end = _parse_any_single_time(parts, is_end=True)
                    if start >= end:
                        raise ValueError("no time selected")
                except ValueError as e:
                    try:
                        # Special handling for single year specifications (e.x. "2003 2004", which should be all of
                        # 2003 only; an alias for 2003-01-01 2004-01-01)
                        if len(parts) != 2:
                            raise ValueError
                        start_year = int(parts[0].strip())
                        end_year = int(parts[1].strip())
                        if 1970 <= start_year <= 2999 and 1970 <= end_year <= 2999 and start_year < end_year:
                            start = datetime.datetime(start_year, 1, 1, tzinfo=datetime.timezone.utc)
                            end = datetime.datetime(end_year, 1, 1, tzinfo=datetime.timezone.utc)
                            return start, end
                    except (ValueError, TypeError, OverflowError):
                        pass
                    raise e
                remaining.clear()

    if not start:
        if not remaining:
            raise ValueError("no start time specification")

        def _parse_start_only():
            nonlocal remaining
            nonlocal start
            nonlocal end

            try:
                # Use the absolute origin, if available, so specifying "1d 2023-05-01" gets the day ending on
                # 2023-05-01, not the day of 2023-05-01.
                if absolute_offset_origin:
                    start = _apply_offset(absolute_offset_origin, _parse_any_offset(remaining[0]) * -1)
                    end = absolute_offset_origin
                else:
                    start = _apply_offset(end, _parse_any_offset(remaining[0]) * -1)
                remaining = remaining[1:]
                return
            except ValueError:
                pass

            try:
                start, _ = _parse_unambiguous_absolute(remaining[0], end.year)
                remaining = remaining[1:]
                return
            except ValueError:
                pass

            start = _parse_any_single_time(_fragment_remaining(), end, is_end=False)
            remaining.clear()

        _parse_start_only()
    elif not end:
        if not remaining:
            raise ValueError("no end time specification")

        def _parse_end_only():
            nonlocal remaining
            nonlocal end

            try:
                end = _apply_offset(start, _parse_any_offset(remaining[-1]))
                remaining = remaining[:-1]
                return
            except ValueError:
                pass

            try:
                # We want to parse through the non-inclusive end here, so that things like "2023-01-01 2024-01-01"
                # yield a single year, instead of a year and a day
                end, _ = _parse_unambiguous_absolute(remaining[-1], start.year)
                remaining = remaining[:-1]
                return
            except ValueError:
                pass

            end = _parse_any_single_time(_fragment_remaining(), start, is_end=True)
            remaining.clear()

        _parse_end_only()

    if remaining:
        raise ValueError("unrecognized extra time arguments")
    return start, end


def parse_interval_argument(s: str) -> float:
    s = s.strip()

    if s.startswith('P'):
        return parse_iso8601_duration(s)

    def valid_number(n: str) -> float:
        n = int(n)
        if n <= 0:
            raise ValueError
        return n

    s = s.lower()

    if s.endswith('s'):
        try:
            return valid_number(s[:-1])
        except ValueError:
            pass
    elif s.endswith('m'):
        try:
            return valid_number(s[:-1]) * 60
        except ValueError:
            pass
    elif s.endswith('h'):
        try:
            return valid_number(s[:-1]) * 60 * 60
        except ValueError:
            pass
    elif s.endswith('d'):
        try:
            return valid_number(s[:-1]) * 24 * 60 * 60
        except ValueError:
            pass
    elif s.endswith('w'):
        try:
            return valid_number(s[:-1]) * 7 * 24 * 60 * 60
        except ValueError:
            pass

    try:
        return valid_number(s)
    except ValueError:
        pass

    raise ValueError("invalid interval format")
