import typing
import asyncio
import datetime
import logging
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from forge.crypto import PublicKey
from forge.dashboard import is_valid_station, is_valid_code
from forge.dashboard.database import (Severity, Entry, Notification, Watchdog, Event, Condition, AccessKey,
                                      AccessBearer, EntryEmail, EventEmail, ConditionEmail)
from forge.dashboard.display import DisplayInterface


_LOGGER = logging.getLogger(__name__)


class ControlInterface(DisplayInterface):
    @staticmethod
    def _code_filter(query, column, code: str):
        if '%' in code:
            return query.filter(column.ilike(code))
        else:
            return query.filter(column == code.lower())

    @staticmethod
    def _to_time(raw, float_seconds: float = 86400):
        if isinstance(raw, datetime.datetime):
            return raw
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        seconds = round(float(raw) * float_seconds)
        return now - datetime.timedelta(seconds=seconds)

    @staticmethod
    def _notification_filter(query, **kwargs):
        if kwargs.get('notification_severity'):
            query = query.filter(Notification.severity == Severity(kwargs['notification_severity']))
        if kwargs.get('notification_code') is not None:
            query = ControlInterface._code_filter(query, Notification.code, kwargs['notification_code'])
        return query

    @staticmethod
    def _watchdog_filter(query, **kwargs):
        query = query.filter(Watchdog.last_seen <= ControlInterface._to_time(kwargs['watchdog_timeout'], 3600))
        if kwargs.get('watchdog_severity'):
            query = query.filter(Watchdog.severity == Severity(kwargs['watchdog_severity']))
        if kwargs.get('watchdog_code') is not None:
            query = ControlInterface._code_filter(query, Watchdog.code, kwargs['watchdog_code'])
        return query

    @staticmethod
    def _event_filter(query, **kwargs):
        if kwargs.get('event_severity'):
            query = query.filter(Event.severity == Severity(kwargs['event_severity']))
        if kwargs.get('event_code') is not None:
            query = ControlInterface._code_filter(query, Event.code, kwargs['event_code'])
        if kwargs.get('before'):
            query = query.filter(Event.occurred_at <= ControlInterface._to_time(kwargs['before']))
        if kwargs.get('after'):
            query = query.filter(Event.occurred_at >= ControlInterface._to_time(kwargs['after']))
        return query

    @staticmethod
    def _condition_filter(query, **kwargs):
        if kwargs.get('condition_severity'):
            query = query.filter(Condition.severity == Severity(kwargs['condition_severity']))
        if kwargs.get('condition_code') is not None:
            query = ControlInterface._code_filter(query, Condition.code, kwargs['condition_code'])
        if kwargs.get('before'):
            query = query.filter(Condition.start_time <= ControlInterface._to_time(kwargs['before']))
        if kwargs.get('after'):
            query = query.filter(Condition.end_time >= ControlInterface._to_time(kwargs['after']))
        return query

    @staticmethod
    def _select_entries(orm_session: Session, **kwargs):
        query = orm_session.query(Entry)

        def prepare_like(raw):
            if '*' in raw:
                return raw.replace('*', '%')
            return f'%{raw}%'

        if kwargs.get('notification_severity') or kwargs.get('notification_code'):
            query = query.join(Notification)
            query = ControlInterface._notification_filter(query, **kwargs)
        if kwargs.get('watchdog_severity') or kwargs.get('watchdog_code'):
            query = query.join(Watchdog)
            query = ControlInterface._watchdog_filter(query, **kwargs)
        if kwargs.get('event_severity') or kwargs.get('event_code'):
            query = query.join(Event)
            query = ControlInterface._event_filter(query, **kwargs)
        if kwargs.get('condition_severity') or kwargs.get('condition_code'):
            query = query.join(Condition)
            query = ControlInterface._condition_filter(query, **kwargs)

        if kwargs.get('entry'):
            query = query.filter(Entry.id == int(kwargs['entry']))
        if kwargs.get('station') is not None:
            station = kwargs['station'].lower()
            if station == '_':
                station = ''
            query = query.filter(Entry.station == station)
        if kwargs.get('entry_code'):
            query = query.filter(Entry.code.ilike(prepare_like(kwargs['entry_code'])))
        if kwargs.get('before'):
            query = query.filter(Entry.updated <= ControlInterface._to_time(kwargs['before']))
        if kwargs.get('after'):
            query = query.filter(Entry.updated >= ControlInterface._to_time(kwargs['after']))
        if kwargs.get('failed') is not None:
            query = query.filter(Entry.failed == bool(kwargs['failed']))

        return query

    @staticmethod
    def _select_access_key(orm_session: Session, **kwargs):
        query = orm_session.query(AccessKey)

        if kwargs.get('access'):
            query = query.filter_by(id=int(kwargs['access']))
        if kwargs.get('station') is not None:
            station = kwargs['station'].lower()
            if station == '_':
                station = ''
            query = query.filter_by(station=station)
        if kwargs.get('entry_code'):
            query = query.filter_by(code=kwargs['entry_code'].lower())
        if kwargs.get('public_key'):
            query = query.filter_by(public_key=kwargs['public_key'])

        return query

    @staticmethod
    def _select_access_bearer(orm_session: Session, **kwargs):
        query = orm_session.query(AccessBearer)

        if kwargs.get('access'):
            query = query.filter_by(id=int(kwargs['access']))
        if kwargs.get('station') is not None:
            station = kwargs['station'].lower()
            if station == '_':
                station = ''
            query = query.filter_by(station=station)
        if kwargs.get('entry_code'):
            query = query.filter_by(code=kwargs['entry_code'].lower())
        if kwargs.get('bearer_token'):
            query = query.filter_by(bearer_token=kwargs['bearer_token'])

        return query

    async def list_filtered(self, include_details: bool = False, **kwargs) -> typing.List[typing.Dict[str, typing.Any]]:
        def execute(engine: Engine):
            result: typing.List[typing.Dict[str, typing.Any]] = list()
            with Session(engine) as orm_session:
                for entry in self._select_entries(orm_session, **kwargs):
                    data = {
                        'id': entry.id,
                        'station': entry.station if entry.station else None,
                        'code': entry.code,
                        'failed': entry.failed,
                        'updated': entry.updated,
                    }

                    if include_details:
                        notifications = list()
                        for add in self._notification_filter(orm_session.query(Notification).filter_by(
                                entry=entry.id), **kwargs):
                            notifications.append({
                                'id': add.id,
                                'code': add.code,
                                'severity': add.severity,
                                'data': add.data,
                            })
                        data['notifications'] = notifications
                        
                        watchdogs = list()
                        for add in self._watchdog_filter(orm_session.query(Watchdog).filter_by(
                                entry=entry.id), **kwargs):
                            watchdogs.append({
                                'id': add.id,
                                'code': add.code,
                                'severity': add.severity,
                                'last_seen': add.last_seen,
                                'data': add.data,
                            })
                        data['watchdogs'] = watchdogs
                        
                        events = list()
                        for add in self._event_filter(orm_session.query(Event).filter_by(
                                entry=entry.id), **kwargs):
                            events.append({
                                'id': add.id,
                                'code': add.code,
                                'severity': add.severity,
                                'occurred_at': add.occurred_at,
                                'data': add.data,
                            })
                        data['events'] = events
                        
                        conditions = list()
                        for add in self._condition_filter(orm_session.query(Condition).filter_by(
                                entry=entry.id), **kwargs):
                            conditions.append({
                                'id': add.id,
                                'code': add.code,
                                'severity': add.severity,
                                'start_time': add.start_time,
                                'end_time': add.end_time,
                                'data': add.data,
                            })
                        data['conditions'] = conditions

                    result.append(data)

            return result

        return await self.db.execute(execute)

    async def remove_entries(self, **kwargs) -> None:
        def execute(engine: Engine):
            with Session(engine) as orm_session:
                for entry in self._select_entries(orm_session, **kwargs):
                    orm_session.query(Entry).filter_by(id=entry.id).delete(synchronize_session=False)
                    _LOGGER.debug(f"Removed entry {entry.station.upper()}/{entry.code}")
                orm_session.commit()

        await self.db.execute(execute)

    async def purge_stale(self, threshold: typing.Union[float, datetime.datetime],
                          watchdogs: bool = True, events: bool = True, conditions: bool = True, **kwargs) -> None:
        threshold = self._to_time(threshold)

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                def apply_watchdogs(query):
                    query = query.filter(Watchdog.last_seen <= threshold)
                    query.delete(synchronize_session=False)

                def apply_events(query):
                    query = query.filter(Event.occurred_at <= threshold)
                    query.delete(synchronize_session=False)

                def apply_conditions(query):
                    query = query.filter(Condition.end_time <= threshold)
                    query.delete(synchronize_session=False)

                entry_query = self._select_entries(orm_session, **kwargs)
                if entry_query.whereclause is None:
                    if watchdogs:
                        apply_watchdogs(orm_session.query(Watchdog))
                        _LOGGER.debug(f"Removing all watchdogs before {threshold}")
                    if events:
                        apply_events(orm_session.query(Event))
                        _LOGGER.debug(f"Removing all events before {threshold}")
                    if conditions:
                        apply_conditions(orm_session.query(Condition))
                        _LOGGER.debug(f"Removing all conditions before {threshold}")
                else:
                    for entry in entry_query:
                        if watchdogs:
                            apply_watchdogs(orm_session.query(Watchdog).filter_by(entry=entry.id))
                            _LOGGER.debug(f"Removing watchdogs for {entry.station.upper()}/{entry.code} before {threshold}")
                        if events:
                            apply_events(orm_session.query(Event).filter_by(entry=entry.id))
                            _LOGGER.debug(f"Removing events for {entry.station.upper()}/{entry.code} before {threshold}")
                        if conditions:
                            apply_conditions(orm_session.query(Condition).filter_by(entry=entry.id))
                            _LOGGER.debug(f"Removing conditions for {entry.station.upper()}/{entry.code} before {threshold}")

                orm_session.commit()

        await self.db.execute(execute)

    async def report_status(self, station: typing.Optional[str], entry_code: str, failed: bool) -> None:
        if not station:
            station = ''
        else:
            station = station.lower()
            if not is_valid_station(station):
                raise ValueError("invalid station code")

        entry_code = entry_code.lower()
        if not is_valid_code(entry_code):
            raise ValueError("invalid entry code")

        now = datetime.datetime.now(tz=datetime.timezone.utc).replace(microsecond=0)

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                entry = orm_session.query(Entry).filter_by(station=station, code=entry_code).one_or_none()
                if entry:
                    entry.failed = failed
                    entry.updated = now

                    _LOGGER.debug(f"Updated entry {entry.station.upper()}/{entry.code}")
                else:
                    entry = Entry(station=station, code=entry_code, failed=failed, updated=now)
                    orm_session.add(entry)

                    _LOGGER.debug(f"Added entry {entry.station.upper()}/{entry.code}")

                orm_session.commit()

        await self.db.execute(execute)

    async def list_access_keys(self, **kwargs) -> typing.List[typing.Dict[str, typing.Any]]:
        def execute(engine: Engine):
            result: typing.List[typing.Dict[str, typing.Any]] = list()
            with Session(engine) as orm_session:
                for access in self._select_access_key(orm_session, **kwargs):
                    result.append({
                        'id': access.id,
                        'public_key': access.public_key,
                        'station': access.station if access.station else None,
                        'code': access.code,
                    })

            return result

        return await self.db.execute(execute)

    async def add_access_key(self, key: PublicKey, station: typing.Optional[str], entry_code: str) -> None:
        key = self.key_to_column(key)

        def execute(engine: Engine):
            result: typing.List[typing.Dict[str, typing.Any]] = list()
            with Session(engine) as orm_session:
                if orm_session.query(AccessKey).filter_by(public_key=key,
                                                          station=station, code=entry_code).one_or_none():
                    _LOGGER.debug(
                        f"Skipping already granted key for {key} on {station.upper()}/{entry_code}")
                    return

                orm_session.add(AccessKey(public_key=key, station=station, code=entry_code))
                _LOGGER.info(f"Adding access for {key} on {station.upper()}/{entry_code}")
                orm_session.commit()

            return result

        await self.db.execute(execute)

    async def remove_access_key(self, **kwargs) -> None:
        def execute(engine: Engine):
            with Session(engine) as orm_session:
                self._select_access_key(orm_session, **kwargs).delete(synchronize_session=False)
                _LOGGER.debug("Removing access key")
                orm_session.commit()

        await self.db.execute(execute)

    async def list_access_bearer(self, **kwargs) -> typing.List[typing.Dict[str, typing.Any]]:
        def execute(engine: Engine):
            result: typing.List[typing.Dict[str, typing.Any]] = list()
            with Session(engine) as orm_session:
                for access in self._select_access_bearer(orm_session, **kwargs):
                    result.append({
                        'id': access.id,
                        'bearer_token': access.bearer_token,
                        'station': access.station if access.station else None,
                        'code': access.code,
                    })

            return result

        return await self.db.execute(execute)

    async def add_access_bearer(self, bearer_token: str, station: typing.Optional[str], entry_code: str) -> None:
        def execute(engine: Engine):
            result: typing.List[typing.Dict[str, typing.Any]] = list()
            with Session(engine) as orm_session:
                if orm_session.query(AccessBearer).filter_by(bearer_token=bearer_token,
                                                             station=station, code=entry_code).one_or_none():
                    _LOGGER.debug(
                        f"Skipping already granted bearer access on {station.upper()}/{entry_code}")
                    return

                orm_session.add(AccessBearer(bearer_token=bearer_token, station=station, code=entry_code))
                _LOGGER.info(f"Adding access bearer access on {station.upper()}/{entry_code}")
                orm_session.commit()

            return result

        await self.db.execute(execute)

    async def remove_access_bearer(self, **kwargs) -> None:
        def execute(engine: Engine):
            with Session(engine) as orm_session:
                self._select_access_bearer(orm_session, **kwargs).delete(synchronize_session=False)
                _LOGGER.debug("Removing bearer token access")
                orm_session.commit()

        await self.db.execute(execute)

    async def email_reset(self, **kwargs) -> None:
        def execute(engine: Engine):
            with Session(engine) as orm_session:
                entry_query = self._select_entries(orm_session, **kwargs)
                if entry_query.whereclause is None:
                    orm_session.query(EntryEmail).delete(synchronize_session=False)
                    orm_session.query(EventEmail).delete(synchronize_session=False)
                    orm_session.query(ConditionEmail).delete(synchronize_session=False)
                    _LOGGER.debug(f"Clearing all email data")
                else:
                    for entry in entry_query:
                        orm_session.query(EntryEmail).filter(entry=entry.id).delete(synchronize_session=False)
                        self._clear_email_pending(orm_session, entry)
                        _LOGGER.debug(f"Clearing email data for {entry.station.upper()}/{entry.code}")

            orm_session.commit()
        await self.db.execute(execute)
