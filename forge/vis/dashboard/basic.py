import typing
import time
import datetime
import starlette.status
from math import ceil, floor
from starlette.requests import Request
from starlette.responses import Response, HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException
from forge.formattime import format_iso8601_time, format_time_of_day, format_date, format_simple_duration
from forge.vis import CONFIGURATION
from forge.vis.util import package_template
from forge.dashboard.display import DisplayInterface
from forge.dashboard.database import (Severity, Entry as DatabaseEntry,
                                      Notification as DatabaseNotification, Watchdog as DatabaseWatchdog,
                                      Event as DatabaseEvent, Condition as DatabaseCondition)
from . import Record as BaseRecord
from .details import (Notification as BaseNotification, Watchdog as BaseWatchdog,
                      Event as BaseEvent, Condition as BaseCondition)
from .status import Status
from .entry import Entry as BaseEntry
from .email import EmailContents
from .badge import assemble_badge_svg, assemble_badge_json


_SEVERITY_SORT: typing.Dict[Severity, int] = {
    Severity.ERROR: 0,
    Severity.WARNING: 1,
    Severity.INFO: 2,
}


class BasicEntry(BaseEntry):
    OFFLINE_THRESHOLD = 26 * 60 * 60

    class Notification(BaseNotification):
        @classmethod
        def from_db(cls, entry: "BasicEntry", db: DatabaseNotification) -> typing.Optional["BasicEntry.Notification"]:
            return cls(entry, db.code, db.severity, db.data)

        @classmethod
        def simple_override(cls, name: typing.Optional[str] = None) -> typing.Type["BasicEntry.Notification"]:
            class Override(BasicEntry.Notification):
                @property
                def display(self) -> str:
                    return name if name else self.code

            return Override

        @property
        def affects_status(self) -> bool:
            return True

    NOTIFICATION_CODES: typing.Dict[str, typing.Type["BasicEntry.Notification"]] = {}

    class Watchdog(BaseWatchdog):
        ALERT_THRESHOLD: typing.Optional[float] = None

        @classmethod
        def from_db(cls, entry: "BasicEntry", db: DatabaseWatchdog) -> typing.Optional["BasicEntry.Watchdog"]:
            last_seen = db.last_seen.replace(tzinfo=datetime.timezone.utc)
            age = (datetime.datetime.now(tz=datetime.timezone.utc) - last_seen).total_seconds()
            alert_threshold = cls.ALERT_THRESHOLD
            if alert_threshold is None:
                alert_threshold = entry.OFFLINE_THRESHOLD
            if not alert_threshold:
                return None
            if age < alert_threshold:
                return None
            if entry.status == BasicEntry.Status.OFFLINE:
                return None
            return cls(entry, db.code, db.severity, last_seen.timestamp(), db.data)

        @classmethod
        def simple_override(cls, name: typing.Optional[str] = None,
                            alert: typing.Optional[float] = None) -> typing.Type["BasicEntry.Watchdog"]:
            class Override(BasicEntry.Watchdog):
                ALERT_THRESHOLD = alert if alert is not None else cls.ALERT_THRESHOLD

                @property
                def display(self) -> str:
                    return name if name else self.code

            return Override

        @property
        def affects_status(self) -> bool:
            return True

    WATCHDOG_CODES: typing.Dict[str, typing.Type["BasicEntry.Watchdog"]] = {}

    class Event(BaseEvent):
        @classmethod
        def from_db(cls, entry: "BasicEntry", db: DatabaseEvent) -> typing.Optional["BasicEntry.Event"]:
            occurred_at = db.occurred_at.replace(tzinfo=datetime.timezone.utc)
            return cls(entry, db.code, db.severity, occurred_at.timestamp(), db.data)

        @classmethod
        def simple_override(cls, name: typing.Optional[str] = None) -> typing.Type["BasicEntry.Event"]:
            class Override(BasicEntry.Event):
                @property
                def display(self) -> str:
                    return name if name else self.code

            return Override

        @property
        def affects_status(self) -> bool:
            return True

    EVENT_CODES: typing.Dict[str, typing.Type["BasicEntry.Event"]] = {}

    class Condition(BaseCondition):
        @classmethod
        def from_db(cls, entry: "BasicEntry", last: DatabaseCondition,
                    begin_present: float, end_present: float,
                    total_seconds: float) -> typing.Optional["BasicEntry.Condition"]:
            return cls(entry, last.code, last.severity, begin_present, end_present, total_seconds, last.data)

        @classmethod
        def simple_override(cls, name: typing.Optional[str] = None) -> typing.Type["BasicEntry.Condition"]:
            class Override(BasicEntry.Condition):
                @property
                def display(self) -> str:
                    return name if name else self.code

            return Override

        @property
        def affects_status(self) -> bool:
            return True

    CONDITION_CODES: typing.Dict[str, typing.Type["BasicEntry.Condition"]] = {}

    @classmethod
    def from_db(cls, station: typing.Optional[str], code: str, db: DatabaseEntry) -> typing.Optional["BasicEntry"]:
        if not db:
            return None
        updated = db.updated.replace(tzinfo=datetime.timezone.utc)
        status = cls.Status.FAILED if db.failed else cls.Status.OK
        if cls.OFFLINE_THRESHOLD:
            age = (datetime.datetime.now(tz=datetime.timezone.utc) - updated).total_seconds()
            if age > cls.OFFLINE_THRESHOLD:
                status = cls.Status.OFFLINE
        return cls(station, code, status, updated.timestamp())

    @classmethod
    def simple_override(cls, name: typing.Optional[str] = None,
                        offline: typing.Optional[float] = None,
                        notifications: typing.Optional[typing.Dict[str,
                            typing.Type["BasicEntry.Notification"]]] = None,
                        watchdogs: typing.Optional[typing.Dict[str,
                            typing.Type["BasicEntry.Watchdog"]]] = None,
                        events: typing.Optional[typing.Dict[str,
                            typing.Type["BasicEntry.Event"]]] = None,
                        conditions: typing.Optional[typing.Dict[str,
                            typing.Type["BasicEntry.Condition"]]] = None) -> typing.Type["BasicEntry"]:
        def override_dict(base, override):
            if not override:
                return base
            result = dict(base)
            result.update(override)
            return result

        class Override(cls):
            OFFLINE_THRESHOLD = offline if offline is not None else cls.OFFLINE_THRESHOLD
            NOTIFICATION_CODES = override_dict(cls.NOTIFICATION_CODES, notifications)
            WATCHDOG_CODES = override_dict(cls.WATCHDOG_CODES, watchdogs)
            EVENT_CODES = override_dict(cls.EVENT_CODES, events)
            CONDITION_CODES = override_dict(cls.CONDITION_CODES, conditions)

            @property
            def display(self) -> typing.Optional[str]:
                return name if name else super().display

        return Override

    def notification_for_code(self, code: str) -> typing.Type["BasicEntry.Notification"]:
        return self.NOTIFICATION_CODES.get(code, self.Notification)

    def notifications(self, raw: typing.Iterable[DatabaseNotification]) -> typing.List["BasicEntry.Notification"]:
        result: typing.List["BasicEntry.Notification"] = list()
        for v in raw:
            add = self.notification_for_code(v.code).from_db(self, v)
            if not add:
                continue
            result.append(add)
        return result

    def watchdog_for_code(self, code: str) -> typing.Type["BasicEntry.Watchdog"]:
        return self.WATCHDOG_CODES.get(code, self.Watchdog)

    def watchdogs(self, raw: typing.Iterable[DatabaseNotification]) -> typing.List["BasicEntry.Watchdog"]:
        result: typing.List["BasicEntry.Watchdog"] = list()
        for v in raw:
            add = self.watchdog_for_code(v.code,).from_db(self, v)
            if not add:
                continue
            result.append(add)
        return result

    def event_for_code(self, code: str) -> typing.Type["BasicEntry.Event"]:
        return self.EVENT_CODES.get(code, self.Event)

    def events(self, raw: typing.Iterable[DatabaseNotification]) -> typing.List["BasicEntry.Event"]:
        result: typing.List["BasicEntry.Event"] = list()
        for v in raw:
            add = self.event_for_code(v.code).from_db(self, v)
            if not add:
                continue
            result.append(add)
        return result

    def condition_for_code(self, code: str) -> typing.Type["BasicEntry.Condition"]:
        return self.CONDITION_CODES.get(code, self.Condition)

    def conditions(self, raw: typing.Iterable[DatabaseCondition],
                   start_epoch: float) -> typing.List["BasicEntry.Condition"]:
        result: typing.List["BasicEntry.Condition"] = list()
        accumulate: typing.Dict[str, typing.Tuple[float, float, float, DatabaseCondition]] = dict()
        for v in raw:
            actual_start = v.start_time.replace(tzinfo=datetime.timezone.utc).timestamp()
            actual_end = v.end_time.replace(tzinfo=datetime.timezone.utc).timestamp()
            effective_start = max(actual_start, start_epoch)
            total_seconds = actual_end - effective_start

            existing = accumulate.get(v.code)
            if not existing:
                accumulate[v.code] = (actual_start, actual_end, total_seconds, v)
            else:
                accumulate[v.code] = (min(actual_start, existing[0]), max(actual_end, existing[1]),
                                      existing[2] + total_seconds, v)

        for begin_present, end_present, total_seconds, last in accumulate.values():
            add = self.condition_for_code(last.code).from_db(self, last, begin_present, end_present, total_seconds)
            if not add:
                continue
            result.append(add)

        return result

    @classmethod
    async def get_status(cls, db: DisplayInterface, email: Status.Email,
                         station: typing.Optional[str], entry_code: str, start_epoch_ms: int,
                         **kwargs) -> typing.Optional["Status"]:
        start_epoch = start_epoch_ms / 1000.0

        def handle_result(data: DisplayInterface.EntryDetails) -> typing.Optional["Status"]:
            entry: typing.Optional[BasicEntry] = cls.from_db(station, entry_code, data.entry)
            if not entry:
                return None

            information_severity: typing.Optional[Severity] = None

            def is_more_severe(severity: Severity) -> bool:
                if information_severity is None:
                    return True
                existing = _SEVERITY_SORT[information_severity]
                incoming = _SEVERITY_SORT[severity]
                return incoming < existing

            for check in entry.notifications(data.notifications):
                if not check.affects_status:
                    continue
                if not is_more_severe(check.severity):
                    continue
                information_severity = check.severity
                if information_severity == Severity.ERROR:
                    return Status(information_severity, email)

            for check in entry.watchdogs(data.watchdogs):
                if not check.affects_status:
                    continue
                if not is_more_severe(check.severity):
                    continue
                information_severity = check.severity
                if information_severity == Severity.ERROR:
                    return Status(information_severity, email)

            for check in entry.events(data.events):
                if not check.affects_status:
                    continue
                if not is_more_severe(check.severity):
                    continue
                information_severity = check.severity
                if information_severity == Severity.ERROR:
                    return Status(information_severity, email)

            for check in entry.conditions(data.conditions, start_epoch):
                if not check.affects_status:
                    continue
                if not is_more_severe(check.severity):
                    continue
                information_severity = check.severity
                if information_severity == Severity.ERROR:
                    return Status(information_severity, email)

            return Status(information_severity, email)

        return await db.entry_details(station, entry_code, start_epoch, handle_result)

    async def base_email_severity(self, **kwargs) -> typing.Optional[Severity]:
        return None


class BasicRecord(BaseRecord):
    DETAILS_TEMPLATE = 'basic.html'
    EMAIL_TEXT_TEMPLATE = 'basic.txt'
    EMAIL_HTML_TEMPLATE = 'basic.html'
    BADGE_TEMPLATE = 'basic.svg'
    ENTRY: typing.Type[BasicEntry] = BasicEntry
    EMAIL_CONTENTS: typing.Type[EmailContents] = EmailContents

    @classmethod
    def simple_override(cls, *args, template_base: typing.Optional[str] = None,
                        **kwargs) -> "BasicRecord":
        class Override(cls):
            DETAILS_TEMPLATE = template_base + ".html" if template_base is not None else cls.DETAILS_TEMPLATE
            EMAIL_TEXT_TEMPLATE = template_base + ".txt" if template_base is not None else cls.EMAIL_TEXT_TEMPLATE
            EMAIL_HTML_TEMPLATE = template_base + ".html" if template_base is not None else cls.EMAIL_HTML_TEMPLATE
            ENTRY = cls.ENTRY.simple_override(*args, **kwargs)
        return Override()

    @staticmethod
    def format_email_time(epoch: float) -> str:
        return format_iso8601_time(epoch)

    async def entry(self, db: DatabaseEntry,
                    station: typing.Optional[str], code: str, **kwargs) -> typing.Optional[BasicEntry]:
        return self.ENTRY.from_db(station, code, db)

    async def status(self, **kwargs) -> typing.Optional[Status]:
        return await self.ENTRY.get_status(**kwargs)

    async def details_data(self, db: DisplayInterface,
                           station: typing.Optional[str], code: str, start_epoch_ms: int) -> typing.Tuple[
            typing.Optional[BasicEntry],
            typing.List[BasicEntry.Notification], typing.List[BasicEntry.Watchdog],
            typing.List[BasicEntry.Event], typing.List[BasicEntry.Condition]]:

        start_epoch = start_epoch_ms / 1000.0

        notifications: typing.List[BasicEntry.Notification] = list()
        watchdogs: typing.List[BasicEntry.Watchdog] = list()
        events: typing.List[BasicEntry.Event] = list()
        conditions: typing.List[BasicEntry.Condition] = list()

        def handle_result(data: DisplayInterface.EntryDetails) -> typing.Optional[BasicEntry]:
            nonlocal notifications
            nonlocal watchdogs
            nonlocal events
            nonlocal conditions

            entry: BasicEntry = self.ENTRY.from_db(station, code, data.entry)
            if not entry:
                return None

            notifications = entry.notifications(data.notifications)
            watchdogs = entry.watchdogs(data.watchdogs)
            events = entry.events(data.events)
            conditions = entry.conditions(data.conditions, start_epoch)
            return entry

        entry = await db.entry_details(station, code, start_epoch, handle_result)
        return entry, notifications, watchdogs, events, conditions

    async def details(self, request: Request, db: DisplayInterface,
                      station: typing.Optional[str], entry_code: str,
                      start_epoch_ms: int, **kwargs) -> Response:
        entry, notifications, watchdogs, events, conditions = await self.details_data(
            db, station, entry_code, start_epoch_ms)
        if not entry:
            raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Entry not found")

        end_epoch = time.time()
        if entry.updated < end_epoch:
            end_epoch = entry.updated
        end_epoch_ms = int(ceil(end_epoch * 1000.0))
        if end_epoch_ms <= start_epoch_ms:
            end_epoch_ms = start_epoch_ms + 1

        notifications.sort(key=lambda x: (_SEVERITY_SORT[x.severity], x.display))
        watchdogs.sort(key=lambda x: (_SEVERITY_SORT[x.severity], x.display))
        conditions.sort(key=lambda x: (_SEVERITY_SORT[x.severity], x.display))
        events.sort(key=lambda x: (x.occurred_at, _SEVERITY_SORT[x.severity], x.display))

        def condition_percent(condition: BasicEntry.Condition) -> str:
            percent = round(condition.total_ms / (end_epoch_ms - start_epoch_ms))
            percent = max(1, min(100, percent))
            return str(percent)

        return HTMLResponse(await package_template('dashboard', 'details', self.DETAILS_TEMPLATE).render_async(
            request=request,
            db=db,
            record=self,
            station=station,
            code=entry_code,
            entry=entry,
            start_epoch_ms=start_epoch_ms,
            end_epoch_ms=end_epoch_ms,
            notifications=notifications,
            watchdogs=watchdogs,
            events=events,
            conditions=conditions,
            Severity=Severity,
            condition_percent=condition_percent,
            format_interval=format_simple_duration,
            format_datetime=format_iso8601_time,
            format_date=format_date,
            format_time=format_time_of_day,
            **kwargs
        ))

    async def email_data(self, db: DisplayInterface,
                         station: typing.Optional[str], code: str, start_epoch_ms: int) -> typing.Tuple[
            typing.Optional[BasicEntry], float, float,
            typing.List[BasicEntry.Notification], typing.List[BasicEntry.Watchdog],
            typing.List[BasicEntry.Event], typing.List[BasicEntry.Condition]]:

        start_epoch = start_epoch_ms / 1000.0
        end_epoch = start_epoch

        notifications: typing.List[BasicEntry.Notification] = list()
        watchdogs: typing.List[BasicEntry.Watchdog] = list()
        events: typing.List[BasicEntry.Event] = list()
        conditions: typing.List[BasicEntry.Condition] = list()

        def handle_result(data: DisplayInterface.EmailDetails) -> typing.Optional[BasicEntry]:
            nonlocal notifications
            nonlocal watchdogs
            nonlocal events
            nonlocal conditions
            nonlocal start_epoch_ms
            nonlocal start_epoch
            nonlocal end_epoch

            entry: BasicEntry = self.ENTRY.from_db(station, code, data.entry)
            if not entry:
                return None

            start_epoch = data.start_time.replace(tzinfo=datetime.timezone.utc).timestamp()
            unsent_time = data.unsent_time
            if unsent_time:
                unsent_time = unsent_time.replace(tzinfo=datetime.timezone.utc).timestamp()
                if start_epoch > unsent_time:
                    start_epoch = unsent_time

            end_epoch = entry.updated

            notifications = entry.notifications(data.notifications)
            watchdogs = entry.watchdogs(data.watchdogs)
            events = entry.events(data.events)
            conditions = entry.conditions(data.conditions, start_epoch)
            return entry

        entry = await db.email_details(station, code, start_epoch, handle_result)
        return entry, start_epoch, end_epoch, notifications, watchdogs, events, conditions

    async def email(self, db: DisplayInterface,
                    station: typing.Optional[str], entry_code: str, start_epoch_ms: int, resend: bool,
                    **kwargs) -> typing.Optional[EmailContents]:
        if resend:
            entry, notifications, watchdogs, events, conditions = await self.details_data(
                db, station, entry_code, start_epoch_ms)
            if not entry:
                return None

            end_epoch = time.time()
            if entry.updated < end_epoch:
                end_epoch = entry.updated
        else:
            entry, start_epoch, end_epoch, notifications, watchdogs, events, conditions = await self.email_data(
                db, station, entry_code, start_epoch_ms)
            if not entry:
                return None

            check = int(floor(start_epoch * 1000.0))
            if start_epoch_ms > check:
                start_epoch_ms = check

        end_epoch_ms = int(ceil(end_epoch * 1000.0))
        if end_epoch_ms <= start_epoch_ms:
            end_epoch_ms = start_epoch_ms + 1

        notifications.sort(key=lambda x: (_SEVERITY_SORT[x.severity], x.display))
        watchdogs.sort(key=lambda x: (_SEVERITY_SORT[x.severity], x.display))
        conditions.sort(key=lambda x: (_SEVERITY_SORT[x.severity], x.display))
        events.sort(key=lambda x: (x.occurred_at, _SEVERITY_SORT[x.severity], x.display))

        contents_severity: typing.Optional[Severity] = await entry.base_email_severity(
            db=db,
            station=station, entry_code=entry_code,
            start_epoch_ms=start_epoch_ms, resend=resend,
            **kwargs
        )

        def apply_contents_severity(severity: Severity) -> None:
            nonlocal contents_severity
            if contents_severity is None:
                contents_severity = severity
                return
            existing = _SEVERITY_SORT[contents_severity]
            incoming = _SEVERITY_SORT[severity]
            if incoming < existing:
                contents_severity = severity

        for check in notifications:
            if not check.affects_status:
                continue
            apply_contents_severity(check.severity)
            break
        for check in watchdogs:
            if not check.affects_status:
                continue
            apply_contents_severity(check.severity)
            break
        for check in conditions:
            if not check.affects_status:
                continue
            apply_contents_severity(check.severity)
            break
        for check in events:
            if not check.affects_status:
                continue
            apply_contents_severity(check.severity)

        def condition_percent(condition: BasicEntry.Condition) -> str:
            percent = condition.total_ms / (end_epoch_ms - start_epoch_ms)
            percent = round(percent * 100.0)
            percent = max(1, min(100, percent))
            return str(percent)

        async def template_file(template) -> str:
            return await template.render_async(
                db=db,
                record=self,
                station=station,
                code=entry_code,
                entry=entry,
                start_epoch_ms=start_epoch_ms,
                end_epoch_ms=end_epoch_ms,
                notifications=notifications,
                watchdogs=watchdogs,
                events=events,
                conditions=conditions,
                contents_severity=contents_severity,
                Severity=Severity,
                URL=CONFIGURATION.get("DASHBOARD.EMAIL.URL"),
                condition_percent=condition_percent,
                format_interval=format_simple_duration,
                format_datetime=format_iso8601_time,
                format_date=format_date,
                format_time=format_time_of_day,
                **kwargs
            )

        text = await template_file(package_template('dashboard', 'email', self.EMAIL_TEXT_TEMPLATE))
        html = await template_file(package_template('dashboard', 'email', self.EMAIL_HTML_TEMPLATE))
        return self.EMAIL_CONTENTS(entry, contents_severity, text, html)

    async def badge_data(self, db: DisplayInterface,
                         station: typing.Optional[str], entry_code: str) -> typing.Optional[BasicEntry]:
        return await self.entry(await db.get_entry(station, entry_code), station, entry_code)

    async def badge_json(self, request: Request, db: DisplayInterface,
                          station: typing.Optional[str], entry_code: str, **kwargs) -> Response:
        entry = await self.badge_data(db, station, entry_code)
        if not entry:
            entry = BasicEntry(station, entry_code, BasicEntry.Status.OFFLINE, time.time())
        return assemble_badge_json(entry)

    async def badge_svg(self, request: Request, db: DisplayInterface,
                          station: typing.Optional[str], entry_code: str, **kwargs) -> Response:
        entry = await self.badge_data(db, station, entry_code)
        if not entry:
            entry = BasicEntry(station, entry_code, BasicEntry.Status.OFFLINE, time.time())
        return await assemble_badge_svg(
            request, entry, self.BADGE_TEMPLATE,
            db=db,
            station=station,
            code=entry_code,
            record=self,
            **kwargs,
        )
