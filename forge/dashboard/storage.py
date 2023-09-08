import typing
import asyncio
import datetime
import logging
import sqlalchemy as db
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from forge.crypto import PublicKey
from forge.vis.access import wildcard_match_level
from . import is_valid_station, is_valid_code
from .database import (Interface, Entry, Notification, Watchdog, Event, EventEmail, Condition, ConditionEmail,
                       AccessKey, AccessBearer)
from .report.action import DashboardAction


_LOGGER = logging.getLogger(__name__)

_match_access = wildcard_match_level


class DashboardInterface(Interface):
    async def apply_action(self, action: DashboardAction) -> None:
        station = action.station
        if not station or station == 'default':
            station = ''
        elif not is_valid_station(station):
            raise ValueError("invalid station")
        entry_code = action.code
        if not is_valid_code(entry_code):
            raise ValueError("invalid code")

        now = datetime.datetime.now(tz=datetime.timezone.utc)
        now = now.replace(microsecond=0)

        if action.update_time:
            update_time = datetime.datetime.fromtimestamp(int(action.update_time), tz=datetime.timezone.utc)
        else:
            update_time = now
        update_time = update_time.replace(microsecond=0)
        if not action.unbounded_time and update_time > now:
            update_time = now

        def execute(engine: Engine) -> None:
            nonlocal update_time

            with Session(engine) as orm_session:
                entry = orm_session.query(Entry).filter_by(station=station, code=entry_code).one_or_none()
                prior_update_time: typing.Optional[datetime.datetime] = None
                if not entry:
                    if action.failed is None:
                        _LOGGER.debug(f"Ignoring partial update to {station.upper()}/{entry_code} because it does not exist")
                        return
                    entry = Entry(station=station, code=entry_code, failed=action.failed, updated=update_time)
                    orm_session.add(entry)
                    orm_session.flush()
                    _LOGGER.debug(f"Creating entry for {station.upper()}/{entry_code}")
                else:
                    prior_update_time = entry.updated.replace(tzinfo=datetime.timezone.utc)
                    if not action.unbounded_time and update_time < prior_update_time:
                        update_time = prior_update_time
                    if action.failed is not None:
                        entry.failed = action.failed
                        entry.updated = update_time
                        _LOGGER.debug(f"Updating state for entry {station.upper()}/{entry_code}")
                    else:
                        _LOGGER.debug(f"Updating information for {station.upper()}/{entry_code}")

                if action.clear_notifications is not None:
                    if action.clear_notifications:
                        orm_session.query(Notification).filter_by(entry=entry.id).filter(db.or_(*[
                            Notification.code == code for code in action.clear_notifications
                        ])).delete(synchronize_session='fetch')
                    _LOGGER.debug("Clearing specified notifications")
                else:
                    orm_session.query(Notification).filter_by(entry=entry.id).delete(synchronize_session='fetch')
                    _LOGGER.debug("Clearing all notifications")

                for a in action.notifications:
                    code = a.code
                    if not code:
                        code = ''
                    elif not is_valid_code(code):
                        raise ValueError("invalid code")

                    target = orm_session.query(Notification).filter_by(entry=entry.id, code=code).one_or_none()
                    if target:
                        target.severity = a.severity
                        target.data = a.data
                        _LOGGER.debug(f"Updated notification {code}")
                    else:
                        target = Notification(entry=entry.id, code=code, severity=a.severity, data=a.data)
                        orm_session.add(target)
                        _LOGGER.debug(f"Added notification {code}")

                if action.clear_watchdogs is not None:
                    if action.clear_watchdogs:
                        orm_session.query(Watchdog).filter_by(entry=entry.id).filter(db.or_(*[
                            Watchdog.code == code for code in action.clear_watchdogs
                        ])).delete(synchronize_session='fetch')
                        _LOGGER.debug(f"Clearing watchdogs")

                for a in action.watchdogs:
                    code = a.code
                    if not is_valid_code(code):
                        raise ValueError("invalid code")
                    last_seen = a.last_seen
                    if last_seen:
                        last_seen = datetime.datetime.fromtimestamp(last_seen, tz=datetime.timezone.utc)
                        last_seen = last_seen.replace(microsecond=0)
                    else:
                        last_seen = update_time

                    target = orm_session.query(Watchdog).filter_by(entry=entry.id, code=code).one_or_none()
                    if target:
                        target.severity = a.severity
                        target.data = a.data
                        target.last_seen = last_seen
                        _LOGGER.debug(f"Updated watchdog {code}")
                    else:
                        target = Watchdog(entry=entry.id, code=code, severity=a.severity, data=a.data,
                                          last_seen=last_seen)
                        orm_session.add(target)
                        _LOGGER.debug(f"Added watchdog {code}")

                for a in action.events:
                    code = a.code
                    if not is_valid_code(code):
                        raise ValueError("invalid code")
                    occurred_at = a.occurred_at
                    if occurred_at:
                        occurred_at = datetime.datetime.fromtimestamp(occurred_at, tz=datetime.timezone.utc)
                        occurred_at = occurred_at.replace(microsecond=0)
                    else:
                        occurred_at = update_time
                    if not action.unbounded_time and occurred_at > now:
                        occurred_at = now

                    target = Event(entry=entry.id, code=code, severity=a.severity, data=a.data,
                                   occurred_at=occurred_at)
                    orm_session.add(target)
                    orm_session.flush()
                    orm_session.add(EventEmail(event=target.id))

                    _LOGGER.debug(f"Added event {code}")

                for a in action.conditions:
                    code = a.code
                    if not is_valid_code(code):
                        raise ValueError("invalid code")
                    end_time = a.end_time
                    if end_time:
                        end_time = datetime.datetime.fromtimestamp(end_time, tz=datetime.timezone.utc)
                        end_time = end_time.replace(microsecond=0)
                    else:
                        end_time = update_time
                    if not action.unbounded_time and end_time > now:
                        end_time = now

                    start_time = a.start_time
                    if start_time:
                        start_time = datetime.datetime.fromtimestamp(start_time, tz=datetime.timezone.utc)
                        start_time = start_time.replace(microsecond=0)
                    else:
                        start_time = prior_update_time
                        if not start_time:
                            start_time = end_time - datetime.timedelta(seconds=1)

                    if not start_time or not end_time or start_time >= end_time:
                        raise ValueError("invalid condition times")

                    first_in_range: typing.Optional[Condition] = None
                    last_end_time: typing.Optional[datetime.datetime] = None
                    query = orm_session.query(Condition).filter_by(entry=entry.id, code=code)
                    query = query.filter(Condition.end_time >= start_time)
                    query = query.filter(Condition.start_time <= end_time)
                    query = query.order_by(Condition.start_time.asc())
                    for existing in query:
                        last_end_time = existing.end_time.replace(tzinfo=datetime.timezone.utc)
                        if not first_in_range:
                            first_in_range = existing
                            continue

                        # Anything other than the first is deleted, since it will be handled by merging
                        orm_session.query(Condition).filter_by(id=existing.id).delete(synchronize_session='fetch')

                    if first_in_range:
                        # Merge existing by extending ranges
                        target = first_in_range
                        if target.start_time.replace(tzinfo=datetime.timezone.utc) > start_time:
                            target.start_time = start_time
                        if target.end_time.replace(tzinfo=datetime.timezone.utc) < end_time:
                            target.end_time = end_time
                        if target.end_time.replace(tzinfo=datetime.timezone.utc) < last_end_time:
                            target.end_time = last_end_time
                        _LOGGER.debug(f"Merged condition {code}")
                    else:
                        target = Condition(entry=entry.id, code=code, severity=a.severity, data=a.data,
                                           start_time=start_time, end_time=end_time)
                        orm_session.add(target)
                        orm_session.flush()
                        _LOGGER.debug(f"Added condition {code}")

                    if not orm_session.query(
                            orm_session.query(ConditionEmail).filter_by(condition=target.id).exists()
                    ).scalar():
                        email_data = ConditionEmail(condition=target.id)
                        orm_session.add(email_data)

                orm_session.commit()

        await self.db.execute(execute)

    async def check_access_key(self, public_key: PublicKey, station: typing.Optional[str], entry_code: str) -> bool:
        key = self.key_to_column(public_key)
        if not station:
            station = ''
        else:
            station = station.lower()
        entry_code = entry_code.lower()

        def execute(engine: Engine) -> bool:
            with Session(engine) as orm_session:
                query = orm_session.query(AccessKey).filter_by(public_key=key)
                query = query.filter(db.or_(AccessKey.station == station, AccessKey.station == '*'))
                for access in query:
                    if _match_access(access.code, entry_code) is not None:
                        return True
            return False

        return await self.db.execute(execute)

    async def check_access_bearer(self, bearer_token: str, station: typing.Optional[str], entry_code: str) -> bool:
        if not station:
            station = ''
        else:
            station = station.lower()
        entry_code = entry_code.lower()

        def execute(engine: Engine) -> bool:
            with Session(engine) as orm_session:
                query = orm_session.query(AccessBearer).filter_by(bearer_token=bearer_token)
                query = query.filter(db.or_(AccessBearer.station == station, AccessBearer.station == '*'))
                for access in query:
                    if _match_access(access.code, entry_code):
                        return True
            return False

        return await self.db.execute(execute)
