import typing
import datetime
import time
import sys
import traceback
from json import dumps
from forge.timeparse import parse_iso8601_time
from forge.formattime import format_iso8601_time
from forge.dashboard import Severity as BaseSeverity, is_valid_station, is_valid_code


class DashboardAction:
    """A container for a dashboard update action

    This class represents an action to be sent to the backend
    to update the status of a single entry (station and code)
    on the dashboard.
    """
    class _Information:
        _ALLOW_EMPTY_CODE = False

        def __init__(self, code: str, severity: "DashboardAction.Severity",
                     data: typing.Optional[typing.Union[str, typing.Dict]] = None):
            if not code and self._ALLOW_EMPTY_CODE:
                self.code: typing.Optional[str] = None
            else:
                if not DashboardAction._is_valid_code(code):
                    raise ValueError(f"invalid code {code}")
                self.code: str = code.lower()
            self.severity = severity
            if data is not None and not isinstance(data, str):
                data = dumps(data)
            if not data:
                data = None
            self.data: typing.Optional[str] = data
            if data and len(data) > 65535:
                raise ValueError("data too long")

        def __str__(self) -> str:
            if not self.data:
                return self.code + ":" + self.severity.name
            return self.code + ":" + self.severity.name + ":" + self.data

        def __repr__(self):
            return "(" + repr(self.code) + ",DashboardAction.Severity." + self.severity.name + "," + repr(self.data) + ")"

    class Notification(_Information):
        """A notification about status at the time of reporting"""
        _ALLOW_EMPTY_CODE = True

        def __eq__(self, other):
            if not isinstance(other, DashboardAction.Notification):
                return NotImplemented
            return self.code == other.code

        def __hash__(self):
            return hash(self.code)

        def __repr__(self):
            return "DashboardAction.Notification" + super().__repr__()

    class Watchdog(_Information):
        """A watchdog that shows when not refreshed"""
        def __init__(self, code: str, severity: "DashboardAction.Severity",
                     data: typing.Optional[typing.Union[str, typing.Dict]] = None,
                     last_seen: typing.Optional[float] = None):
            super().__init__(code, severity, data)
            self.last_seen: typing.Optional[float] = last_seen

        def __eq__(self, other):
            if not isinstance(other, DashboardAction.Watchdog):
                return NotImplemented
            return self.code == other.code

        def __hash__(self):
            return hash(self.code)

        def __repr__(self):
            return "DashboardAction.Watchdog" + super().__repr__()

    class Event(_Information):
        """An event at a point in time"""
        def __init__(self, code: str, severity: "DashboardAction.Severity",
                     data: typing.Optional[typing.Union[str, typing.Dict]] = None,
                     occurred_at: typing.Optional[float] = None):
            super().__init__(code, severity, data)
            self.occurred_at = occurred_at

        def __str__(self) -> str:
            result = self.code + ":" + DashboardAction._display_time(self.occurred_at) + ":" + self.severity.name
            result += ":" + self.severity.name
            if self.data:
                result += ":" + self.data
            return result

        def __repr__(self):
            return ("DashboardAction.Event(" + repr(self.code) +
                    ",DashboardAction.Severity." + self.severity.name + "," +
                    repr(self.data) + "," +
                    (repr(datetime.datetime.fromtimestamp(self.occurred_at, tz=datetime.timezone.utc)) if self.occurred_at else "None") +
                    ")")

    class Condition(_Information):
        """A condition spanning a continuous range of time"""
        def __init__(self, code: str, severity: "DashboardAction.Severity",
                     data: typing.Optional[typing.Union[str, typing.Dict]] = None,
                     start_time: typing.Optional[float] = None, end_time: typing.Optional[float] = None):
            super().__init__(code, severity, data)
            if start_time and end_time and start_time >= end_time:
                raise ValueError("start time must be before end time")
            self.start_time = start_time
            self.end_time = end_time

        def __str__(self) -> str:
            result = self.code + ":" + DashboardAction._display_time(self.start_time) + ":"
            result += DashboardAction._display_time(self.end_time) + ":" + self.severity.name
            if self.data:
                result += ":" + self.data
            return result

        def __repr__(self):
            return ("DashboardAction.Condition(" + repr(self.code) +
                    ",DashboardAction.Severity." + self.severity.name + "," +
                    repr(self.data) + "," +
                    (repr(datetime.datetime.fromtimestamp(self.start_time, tz=datetime.timezone.utc)) if self.start_time else "None") + "," +
                    (repr(datetime.datetime.fromtimestamp(self.end_time, tz=datetime.timezone.utc)) if self.start_time else "None") +
                    ")")

    def __init__(self, station: typing.Optional[str], code: str):
        """Initialize an empty reporting action

        The created action will change no state unless the various members are set

        Attributes:
            station (str): The station code, if applicable
            code (str): The dashboard entry code, if applicable
            update_time (float): The time of update, if unset then the current time is used
            failed (bool): If set, then set the failure state
            notifications (set[DashboardAction.Notification]): Any notification to set
            clear_notifications (set[str]): If set, then only these notification are removed, instead of all prior ones
            watchdogs (set[DashboardAction.Watchdog]): Any watchdogs to start or restart
            clear_watchdogs (set[str]): Watchdogs to stop
            events (list[DashboardAction.Event]): Events to add
            conditions (list[DashboardAction.Condition]): Conditions to apply

        :param station: reporting station code
        :param code: dashboard entry code
        """
        if station and not self._is_valid_station(station):
            raise ValueError("invalid station")
        if not self._is_valid_code(code):
            raise ValueError("invalid entry code")
        self.station = station

        if self.station:
            self.station = self.station.lower()
            if self.station == 'default':
                self.station = None
        self.code = code.lower()

        self.update_time: typing.Optional[float] = None
        self.unbounded_time: bool = False
        self.failed: typing.Optional[bool] = None

        self.notifications: typing.Set[DashboardAction.Notification] = set()
        self.clear_notifications: typing.Optional[typing.Set[str]] = None

        self.watchdogs: typing.Set[DashboardAction.Watchdog] = set()
        self.clear_watchdogs: typing.Set[str] = set()

        self.events: typing.List[DashboardAction.Event] = list()

        self.conditions: typing.List[DashboardAction.Condition] = list()

    @classmethod
    def from_args(cls, station: typing.Optional[str], code: str, **kwargs) -> "DashboardAction":
        """Construct a dashboard action from arguments.

        See the module level documentation for the arguments accepted.

        :param station: station code, if applicable
        :param code: dashboard internal code
        :param kwargs: arguments to assemble
        :return: the action assembled from the kwargs
        """
        act = cls(station, code)

        def to_time(raw) -> typing.Optional[float]:
            if not raw:
                return None
            if isinstance(raw, bytes) or isinstance(raw, bytearray):
                raw = raw.decode('ascii')
            if isinstance(raw, str):
                return cls._parse_time(raw).timestamp()
            if isinstance(raw, datetime.datetime):
                return raw.timestamp()
            if raw:
                return float(raw)
            return None

        if 'update_time' in kwargs:
            act.update_time = to_time(kwargs['update_time'])
            if act.update_time is not None:
                if act.update_time:
                    if act.update_time < 2.0:
                        act.update_time = time.time()
                else:
                    act.update_time = None
        act.unbounded_time = bool(kwargs.get('unbounded_time'))

        if 'failed' in kwargs:
            act.failed = kwargs['failed']
            if act.failed is not None:
                act.failed = bool(act.failed)

        def to_iterable(raw, part_split) -> typing.Union[typing.Iterable, typing.Mapping]:
            if isinstance(raw, bytes) or isinstance(raw, bytearray):
                raw = raw.decode('ascii')
            if isinstance(raw, str):
                if part_split:
                    raw = raw.split(part_split)
                else:
                    raw = [raw]
            return raw

        def to_severity(raw, default) -> "DashboardAction.Severity":
            if raw is None:
                return default
            if isinstance(raw, cls.Severity):
                return raw
            return cls.Severity(raw.lower())

        def parts_or_dict(raw, *args):
            result = list()
            if isinstance(raw, dict):
                for a in args:
                    result.append(raw.get(a))
                return tuple(result)
            i = iter(raw)
            try:
                for a in range(len(args)):
                    result.append(next(i))
            except StopIteration:
                pass
            while len(result) < len(args):
                result.append(None)
            return tuple(result)

        if kwargs.get('exc_info'):
            _, exc, tb = sys.exc_info()
            if exc is not None:
                lines = traceback.format_exception(exc, value=exc, tb=tb)
                act.notifications.add(cls.Notification('', cls.Severity.ERROR, ''.join(lines)))

        if kwargs.get('notifications'):
            for raw in to_iterable(kwargs['notifications'], ','):
                if isinstance(raw, act.Notification):
                    act.notifications.add(raw)
                    continue
                code, severity, data = parts_or_dict(to_iterable(raw, ':'), 'code', 'severity', 'data')
                severity = to_severity(severity, cls.Severity.ERROR)
                act.notifications.add(cls.Notification(code, severity, data))
        if kwargs.get('preserve_existing_notifications'):
            act.clear_notifications = set()
        if 'notifications_to_clear' in kwargs:
            act.clear_notifications = set()
            for notification in to_iterable(kwargs['notifications_to_clear'], ','):
                act.clear_notifications.add(str(notification).lower())

        if kwargs.get('watchdogs'):
            for raw in to_iterable(kwargs['watchdogs'], ','):
                if isinstance(raw, act.Watchdog):
                    act.watchdogs.add(raw)
                    continue
                code, severity, data, last_seen = parts_or_dict(to_iterable(raw, ':'),
                                                                'code', 'severity', 'data', 'last_seen')
                severity = to_severity(severity, cls.Severity.ERROR)
                last_seen = to_time(last_seen)
                act.watchdogs.add(cls.Watchdog(code, severity, data, last_seen))
        if 'watchdogs_to_clear' in kwargs:
            for watchdog in to_iterable(kwargs['watchdogs_to_clear'], ','):
                act.clear_watchdogs.add(str(watchdog).lower())

        if kwargs.get('events'):
            for raw in to_iterable(kwargs['events'], ','):
                if isinstance(raw, act.Event):
                    act.events.append(raw)
                    continue
                code, occurred_at, severity, data = parts_or_dict(to_iterable(raw, ':'),
                                                                  'code', 'occurred_at', 'severity', 'data')
                occurred_at = to_time(occurred_at)
                severity = to_severity(severity, cls.Severity.ERROR)
                act.events.append(cls.Event(code, severity, data, occurred_at))

        if kwargs.get('conditions'):
            for raw in to_iterable(kwargs['conditions'], ','):
                if isinstance(raw, act.Condition):
                    act.conditions.append(raw)
                    continue
                code, start_time, end_time, severity, data = parts_or_dict(
                    to_iterable(raw, ':'), 'code', 'start_time', 'end_time', 'severity', 'data')
                start_time = to_time(start_time)
                end_time = to_time(end_time)
                severity = to_severity(severity, cls.Severity.ERROR)
                act.conditions.append(cls.Condition(code, severity, data, start_time, end_time))

        return act

    def to_json(self) -> typing.Dict[str, typing.Any]:
        """Convert the action into JSON for uploading

        :return: a dict suitable for JSON encoding and upload
        """
        result: typing.Dict[str, typing.Any] = {
            'code': self.code
        }
        if self.station:
            result['station'] = self.station
        if self.update_time:
            result['update_time'] = int(round(self.update_time))
        if self.unbounded_time:
            result['unbounded_time'] = 1
        if self.failed is not None:
            result['status'] = 'failed' if self.failed else 'ok'

        def information_to_dict(info: DashboardAction._Information) -> typing.Dict[str, typing.Any]:
            r = {
                'code': info.code or '',
                'severity': info.severity.value,
            }
            if info.data:
                r['data'] = info.data
            return r

        if self.notifications:
            result['notifications'] = [information_to_dict(a) for a in self.notifications]
        if self.clear_notifications is not None:
            result['clear_notifications'] = [a for a in self.clear_notifications]

        def watchdog_to_dict(watchdog: DashboardAction.Watchdog) -> typing.Dict[str, typing.Any]:
            r = information_to_dict(watchdog)
            if watchdog.last_seen:
                r['last_seen'] = int(round(watchdog.last_seen))
            return r

        if self.watchdogs:
            result['watchdogs'] = [watchdog_to_dict(a) for a in self.watchdogs]
        if self.clear_watchdogs:
            result['clear_watchdogs'] = [a for a in self.clear_watchdogs]

        def event_to_dict(event: DashboardAction.Event) -> typing.Dict[str, typing.Any]:
            r = information_to_dict(event)
            if event.occurred_at:
                r['occurred_at'] = int(round(event.occurred_at))
            return r

        if self.events:
            result['events'] = [event_to_dict(a) for a in self.events]

        def condition_to_dict(condition: DashboardAction.Condition) -> typing.Dict[str, typing.Any]:
            r = information_to_dict(condition)
            if condition.start_time:
                r['start_time'] = int(round(condition.start_time))
            if condition.end_time:
                r['end_time'] = int(round(condition.end_time))
            return r

        if self.conditions:
            result['conditions'] = [condition_to_dict(a) for a in self.conditions]

        return result

    def __str__(self) -> str:
        result = "DashboardAction("
        if self.station:
            result += self.station.upper() + " "
        result += self.code + ")"
        if self.update_time:
            result += " @ " + DashboardAction._display_time(self.update_time)
        if self.failed is not None:
            if self.failed:
                result += " FAILED"
            else:
                result += " OK"
        if not self.notifications and not self.watchdogs and not self.events and not self.conditions:
            return result
        result += ":\n"

        if self.notifications:
            to_sort: typing.List["DashboardAction.Notification"] = list()
            status_notification: typing.Optional["DashboardAction.Notification"] = None
            for n in self.notifications:
                if n.code:
                    to_sort.append(n)
                else:
                    status_notification = n
            if status_notification and status_notification.data:
                result += "    STATUS: " + status_notification.code + ":" + status_notification.data + "\n"
            if to_sort:
                to_sort.sort(key=lambda x: x.code)
                result += "    NOTIFICATIONS:\n"
                for i in to_sort:
                    result += "        " + str(i) + "\n"

        if self.watchdogs:
            result += "    WATCHDOGS:\n"
            for i in sorted(self.watchdogs, key=lambda x: x.code):
                result += "        " + str(i) + "\n"

        if self.events:
            result += "    EVENTS:\n"
            for i in sorted(self.events, key=lambda x: x.code):
                result += "        " + str(i) + "\n"

        if self.conditions:
            result += "    CONDITIONS:\n"
            for i in sorted(self.conditions, key=lambda x: x.code):
                result += "        " + str(i) + "\n"

        return result.strip()

    def __repr__(self) -> str:
        result = "DashboardAction("
        if self.station:
            result += f'station="{self.station.upper()}", '
        else:
            result += "station=None, "
        result += f'code="{self.code}", '
        if self.update_time:
            result += f'update_time={repr(datetime.datetime.fromtimestamp(self.update_time, tz=datetime.timezone.utc))}, '
        if self.unbounded_time:
            result += "unbounded_time=True, "
        if self.failed is not None:
            if self.failed:
                result += "failed=True, "
            else:
                result += "failed=False, "
        result += "notifications=" + repr(sorted(self.notifications, key=lambda x: x.code)) + ", "
        result += "watchdogs=" + repr(sorted(self.watchdogs, key=lambda x: x.code)) + ", "
        result += "events=" + repr(sorted(self.events, key=lambda x: x.code)) + ", "
        result += "conditions=" + repr(sorted(self.conditions, key=lambda x: x.code)) + ")"

        return result

    Severity = BaseSeverity

    @staticmethod
    def _is_valid_code(code: str):
        return is_valid_code(code)

    @staticmethod
    def _is_valid_station(station: str):
        return is_valid_station(station)

    @staticmethod
    def _parse_time(t: str):
        return parse_iso8601_time(t)

    @staticmethod
    def _display_time(t: float):
        if not t:
            return ""
        return format_iso8601_time(t)
