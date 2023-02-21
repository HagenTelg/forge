import typing
from .entry import Entry
from .status import Status
from .basic import BasicEntry, BasicRecord, Severity


class _ExampleEntry(BasicEntry):
    class Notification(BasicEntry.Notification):
        @property
        def display(self) -> str:
            if self.severity == Severity.INFO:
                return "Example Details Info"
            return super().display

    class Watchdog(BasicEntry.Watchdog):
        @property
        def display(self) -> str:
            if self.severity == Severity.INFO:
                return "Example Details Info"
            return super().display

    class Event(BasicEntry.Event):
        @property
        def display(self) -> str:
            if self.severity == Severity.INFO:
                return "Example Details Info"
            return super().display

    class Condition(BasicEntry.Condition):
        @property
        def display(self) -> str:
            if self.severity == Severity.INFO:
                return "Example Details Info"
            return super().display

    @property
    def display(self) -> typing.Optional[str]:
        if self.code == 'example-nominal':
            return "Example Nominal Entry"
        elif self.code == 'example-offline':
            return "Example Offline Entry"
        elif self.code == 'example-failed':
            return "Example Failed Entry with an extra long name"
        return None

    @classmethod
    def from_db(cls, station: typing.Optional[str], code: str, _) -> typing.Optional["_ExampleEntry"]:
        if code == 'example-offline':
            status = cls.Status.OFFLINE
        elif code == 'example-failed':
            status = cls.Status.FAILED
        else:
            status = cls.Status.OK
        return cls(station, code, status, 1677024000)


example_list = [
    _ExampleEntry('bnd', 'example-nominal', Entry.Status.OK, 1677024000),
    _ExampleEntry('alt', 'example-offline', Entry.Status.OFFLINE, 1677024000),
    _ExampleEntry('alt', 'example-failed', Entry.Status.FAILED, 1677024000),
    _ExampleEntry(None, 'example-no-station', Entry.Status.OK, 1677024000),
]


class _ExampleRecord(BasicRecord):
    ENTRY = _ExampleEntry

    async def entry(self, station: typing.Optional[str], code: str, **kwargs) -> typing.Optional[BasicEntry]:
        if code == 'example-offline':
            return _ExampleEntry(station, code, Entry.Status.OFFLINE, 1677024000)
        elif code == 'example-failed':
            return _ExampleEntry(station, code, Entry.Status.FAILED, 1677024000)
        return _ExampleEntry(station, code, Entry.Status.OK, 1677024000)

    async def status(self, email: Status.Email,
                     station: typing.Optional[str], entry_code: str, start_epoch_ms: int,
                     **kwargs) -> typing.Optional[Status]:
        if entry_code == 'example-offline':
            return Status(Severity.ERROR, Status.Email.ERROR)
        elif entry_code == 'example-failed':
            return Status(Severity.WARNING, Status.Email.WARNING)
        elif entry_code == 'example-no-station':
            return Status(Severity.INFO, Status.Email.INFO)

        return Status(None, Status.Email.OFF)

    async def details_data(self, _,
                           station: typing.Optional[str], code: str, start_epoch_ms: int) -> typing.Tuple[
            typing.Optional[BasicEntry],
            typing.List[BasicEntry.Notification], typing.List[BasicEntry.Watchdog],
            typing.List[BasicEntry.Event], typing.List[BasicEntry.Condition]]:
        entry = _ExampleEntry(station, code, Entry.Status.OK, 1677024000)
        return (
            entry,
            [
                _ExampleEntry.Notification(entry, "example-notification-info", Severity.INFO),
                _ExampleEntry.Notification(entry, "example-notification-warning", Severity.WARNING,
                                           "Warning notification"),
                _ExampleEntry.Notification(entry, "example-notification-error", Severity.ERROR,
                                           "Error notification"),
                *([_ExampleEntry.Notification(entry, "", Severity.ERROR, "Global status notification")]
                  if code == "example-failed" else []),
            ],
            [
                _ExampleEntry.Watchdog(entry, "example-watchdog-info", Severity.INFO, 1677024000,
                                       "Info watchdog"),
                _ExampleEntry.Watchdog(entry, "example-watchdog-warning", Severity.WARNING, 1677024000),
                _ExampleEntry.Watchdog(entry, "example-watchdog-error", Severity.ERROR, 1677024000,
                                       "Error watchdog"),
            ],
            [
                _ExampleEntry.Event(entry, "example-event-info", Severity.INFO, 1677024000,
                                    "Info event"),
                _ExampleEntry.Event(entry, "example-event-info", Severity.INFO, 1677024001,
                                    "Info event 2"),
                _ExampleEntry.Event(entry, "example-event-warning", Severity.WARNING, 1677024010),
                _ExampleEntry.Event(entry, "example-event-warning", Severity.WARNING, 1677024011),
                _ExampleEntry.Event(entry, "example-event-error", Severity.ERROR, 1677024300,
"""
Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.
Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
""".strip()),
                _ExampleEntry.Event(entry, "example-event-error", Severity.ERROR, 1677024301),
            ],
            [
                _ExampleEntry.Condition(entry, "example-condition-info", Severity.INFO,
                                        1677024000, 1677024000 + 1 * 60 * 60, 1 * 60 * 60, """
Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.
Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
""".replace("\n", " ").strip()),
                _ExampleEntry.Condition(entry, "example-condition-warning", Severity.WARNING,
                                        1677024000, 1677024000 + 2 * 60 * 60, 1 * 60 * 60),
                _ExampleEntry.Condition(entry, "example-condition-error", Severity.ERROR,
                                        1677024000, 1677024000 + 3 * 60 * 60, 1 * 60 * 60),
            ],
        )

    async def email_data(self, _,
                         station: typing.Optional[str], code: str, start_epoch_ms: int) -> typing.Tuple[
            typing.Optional[BasicEntry], float, float,
            typing.List[BasicEntry.Notification], typing.List[BasicEntry.Watchdog],
            typing.List[BasicEntry.Event], typing.List[BasicEntry.Condition]]:
        entry, notifications, watchdogs, events, conditions = await self.details_data(
            None, station, code, start_epoch_ms)
        start_epoch = start_epoch_ms / 1000.0
        end_epoch = start_epoch + 24 * 60 * 60
        return entry, start_epoch, end_epoch, notifications, watchdogs, events, conditions

    async def badge_data(self, _,
                         station: typing.Optional[str], code: str) -> typing.Optional[BasicEntry]:
        if code == 'example-offline':
            return _ExampleEntry(station, code, Entry.Status.OFFLINE, 1677024000)
        elif code == 'example-failed':
            return _ExampleEntry(station, code, Entry.Status.FAILED, 1677024000)

        return _ExampleEntry(station, code, Entry.Status.OK, 1677024000)


example_record = _ExampleRecord()

