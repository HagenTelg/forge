import typing
from math import floor, ceil
from forge.dashboard.database import Severity
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


class Event(_CodedType):
    def __init__(self, entry: Entry, code: str, severity: Severity, occurred_at: float,
                 data: typing.Optional[str] = None):
        super().__init__(entry, code, severity, data)
        self.occurred_at = occurred_at

    @property
    def occurred_at_ms(self) -> int:
        return int(round(self.occurred_at * 1000.0))


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
    def end_present_ms(self) -> int:
        return int(ceil(self.end_present * 1000.0))

    @property
    def total_ms(self) -> int:
        return int(ceil(self.total_seconds * 1000.0))
