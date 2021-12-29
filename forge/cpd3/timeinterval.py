import typing
from enum import IntEnum
from math import isfinite


class TimeUnit(IntEnum):
    Millisecond = 0
    Second = 1
    Minute = 2
    Hour = 3
    Day = 4
    Week = 5
    Month = 6
    Quarter = 7
    Year = 8

    @staticmethod
    def from_string(unit: str) -> "TimeUnit":
        unit = unit.lower()
        if unit == 'millisecond' or unit == 'milliseconds' or unit == 'msec':
            return TimeUnit.Millisecond
        elif unit == 'second' or unit == 'seconds' or unit == 'sec' or unit == 'secs' or unit == 's':
            return TimeUnit.Second
        elif unit == 'minute' or unit == 'minutes' or unit == 'min' or unit == 'mins' or unit == 'm' or unit == 'mi':
            return TimeUnit.Minute
        elif unit == 'hour' or unit == 'hours' or unit == 'h':
            return TimeUnit.Hour
        elif unit == 'day' or unit == 'days' or unit == 'd':
            return TimeUnit.Day
        elif unit == 'week' or unit == 'weeks' or unit == 'w':
            return TimeUnit.Week
        elif unit == 'month' or unit == 'months' or unit == 'mon' or unit == 'mons' or unit == 'mo':
            return TimeUnit.Month
        elif unit == 'quarter' or unit == 'quarters' or unit == 'qtr' or unit == 'qtrs' or unit == 'q':
            return TimeUnit.Quarter
        elif unit == 'year' or unit == 'years' or unit == 'y':
            return TimeUnit.Year
        return TimeUnit.Second


class TimeInterval:
    def __init__(self, unit: typing.Optional[typing.Union[TimeUnit, "TimeInterval"]] = None,
                 count: typing.Optional[int] = None, align: typing.Optional[bool] = None):
        if isinstance(unit, TimeInterval):
            self.unit = unit.unit
            if count is not None:
                self.count = count
            else:
                self.count = unit.count
            if align is not None:
                self.align = align
            else:
                self.align = unit.align
        elif isinstance(unit, TimeUnit):
            self.unit = unit
            if count is not None:
                self.count = count
            else:
                self.count = 1
            if align is not None:
                self.align = align
            else:
                self.align = False
        else:
            self.unit = unit
            self.count = count
            self.align = align

    @staticmethod
    def from_variant(value: typing.Optional[typing.Union[typing.Dict[str, typing.Any], str, int, float]],
                     reference: typing.Optional["TimeInterval"] = None) -> "TimeInterval":
        if value is None:
            return TimeInterval(reference)
        if isinstance(value, str):
            result = TimeInterval(TimeUnit.from_string(value))
            if reference:
                if reference.count is not None:
                    result.count = reference.count
                if reference.align is not None:
                    result.align = reference.align
            return result
        elif isinstance(value, int) or isinstance(value, float):
            if isinstance(value, float):
                if not isfinite(value):
                    value = None
            return TimeInterval(reference, value)

        count = value.get('Count')
        if count is None:
            count = value.get('Number')
        if count is None:
            count = value.get('Offset')
        if count is None:
            count = value.get('N')
        if isinstance(count, float):
            if not isfinite(count):
                count = None
            else:
                count = int(count)
        elif not isinstance(count, int):
            count = None

        align = value.get('Align')
        if align is None:
            align = value.get('Aligned')
        if not isinstance(align, bool):
            align = None

        unit = value.get('Units')
        if unit is None:
            unit = value.get('Unit')
        if not isinstance(unit, str):
            unit = None

        result = TimeInterval(reference, count, align)
        if unit is not None:
            result.unit = TimeUnit.from_string(unit)
        return result
