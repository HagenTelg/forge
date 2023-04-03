import typing
from math import floor, ceil
from forge.dashboard.database import Severity
from forge.formattime import format_iso8601_time, format_simple_duration
from .entry import Entry


class _CodedType:
    def __init__(self, entry: Entry, code: str, severity: Severity, data: typing.Optional[str] = None):
        self.entry = entry
        self.code = code
        self.severity = severity
        self.data = data

    @property
    def display(self) -> str:
        return self.code

    @property
    def detail(self) -> typing.Optional[str]:
        return self.data


class Notification(_CodedType):
    pass


class Watchdog(_CodedType):
    def __init__(self, entry: Entry, code: str, severity: Severity, last_seen: float,
                 data: typing.Optional[str] = None):
        super().__init__(entry, code, severity, data)
        self.last_seen = last_seen

    @property
    def last_seen_ms(self) -> int:
        return int(round(self.last_seen * 1000.0))

    @property
    def last_seen_iso8601(self) -> str:
        return format_iso8601_time(self.last_seen)


class Event(_CodedType):
    def __init__(self, entry: Entry, code: str, severity: Severity, occurred_at: float,
                 data: typing.Optional[str] = None):
        super().__init__(entry, code, severity, data)
        self.occurred_at = occurred_at

    @property
    def occurred_at_ms(self) -> int:
        return int(round(self.occurred_at * 1000.0))

    @property
    def occurred_at_iso8601(self) -> str:
        return format_iso8601_time(self.occurred_at)


class Condition(_CodedType):
    def __init__(self, entry: Entry, code: str, severity: Severity, begin_present: float, end_present: float,
                 total_seconds: float, data: typing.Optional[str] = None):
        super().__init__(entry, code, severity, data)
        self.begin_present = begin_present
        self.end_present = end_present
        self.total_seconds = total_seconds

    @property
    def begin_present_ms(self) -> int:
        return int(floor(self.begin_present * 1000.0))

    @property
    def begin_present_iso8601(self) -> str:
        return format_iso8601_time(self.begin_present)

    @property
    def end_present_ms(self) -> int:
        return int(ceil(self.end_present * 1000.0))

    @property
    def end_present_iso8601(self) -> str:
        return format_iso8601_time(self.end_present)

    @property
    def total_ms(self) -> int:
        return int(ceil(self.total_seconds * 1000.0))

    @property
    def total_display(self) -> str:
        return format_simple_duration(self.total_seconds)
