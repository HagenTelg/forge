import typing
import logging
import asyncio
import datetime
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from .database import Interface, Entry, Notification, Watchdog, Event, Condition, EntryEmail, EventEmail, ConditionEmail


_LOGGER = logging.getLogger(__name__)


class DisplayInterface(Interface):
    async def entry_exists(self, station: typing.Optional[str], entry_code: str) -> bool:
        if station is None:
            station = ''

        def execute(engine: Engine) -> typing.Optional[Entry]:
            with Session(engine) as orm_session:
                return orm_session.query(
                    orm_session.query(Entry).filter_by(station=station, code=entry_code).exists()
                ).scalar()

        return await self.db.execute(execute)

    async def get_entry(self, station: typing.Optional[str], entry_code: str) -> typing.Optional[Entry]:
        if station is None:
            station = ''

        def execute(engine: Engine) -> typing.Optional[Entry]:
            with Session(engine) as orm_session:
                return orm_session.query(Entry).filter_by(station=station.lower(),
                                                          code=entry_code.lower()).one_or_none()

        return await self.db.execute(execute)

    class EntryDetails:
        def __init__(self, session: Session, start_date: datetime.datetime, entry: Entry):
            self._session = session
            self._start_date = start_date
            self.entry = entry

        @property
        def notifications(self) -> typing.Iterable[Notification]:
            return self._session.query(Notification).filter_by(entry=self.entry.id)

        @property
        def watchdogs(self) -> typing.Iterable[Watchdog]:
            return self._session.query(Watchdog).filter_by(entry=self.entry.id)

        @property
        def events(self) -> typing.Iterable[Event]:
            query = self._session.query(Event).filter_by(entry=self.entry.id)
            query = query.filter(Event.occurred_at >= self._start_date)
            query = query.order_by(Event.occurred_at.desc())
            return query

        @property
        def conditions(self) -> typing.Iterable[Condition]:
            query = self._session.query(Condition).filter_by(entry=self.entry.id)
            query = query.filter(Condition.end_time >= self._start_date)
            query = query.order_by(Condition.start_time.desc())
            return query

    async def entry_details(self, station: typing.Optional[str], entry_code: str, start_epoch: float,
                            result: typing.Callable[["DisplayInterface.EntryDetails"], None]):
        if station is None:
            station = ''
        start_date = datetime.datetime.fromtimestamp(int(start_epoch), tz=datetime.timezone.utc)

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                entry = orm_session.query(Entry).filter_by(station=station, code=entry_code).one_or_none()
                if entry is None:
                    return None
                context = self.EntryDetails(orm_session, start_date, entry)
                v = result(context)
                context._session = None
                return v

        return await self.db.execute(execute)

    class EmailDetails(EntryDetails):
        def __init__(self, session: Session, start_date: datetime.datetime, entry: Entry,
                     unsent_updated: typing.Optional[datetime.datetime],
                     last_send: typing.Optional[datetime.datetime]):
            if last_send and last_send < start_date:
                start_date = last_send

            super().__init__(session, start_date, entry)
            self._unsent_updated = unsent_updated

        @property
        def events(self) -> typing.Iterable[Event]:
            unsent = self._session.query(Event).filter_by(entry=self.entry.id)
            unsent = unsent.join(EventEmail)

            unseen = self._session.query(Event).filter_by(entry=self.entry.id)
            unseen = unseen.filter(Event.occurred_at >= self._start_date)

            query = unsent.union(unseen)
            query = query.order_by(Event.occurred_at.desc())
            return query

        @property
        def conditions(self) -> typing.Iterable[Condition]:
            unsent = self._session.query(Condition).filter_by(entry=self.entry.id)
            unsent = unsent.join(ConditionEmail)

            unseen = self._session.query(Condition).filter_by(entry=self.entry.id)
            unseen = unseen.filter(Condition.end_time >= self._start_date)

            query = unsent.union(unseen)
            query = query.order_by(Condition.start_time.desc())
            return query

        @property
        def start_time(self) -> typing.Optional[datetime.datetime]:
            return self._start_date

        @property
        def unsent_time(self) -> typing.Optional[datetime.datetime]:
            return self._unsent_updated

    @staticmethod
    def _clear_email_pending(orm_session: Session, entry: Entry):
        # Multiple-table delete criteria not supported on SQLite, so have to do this manually
        for email in orm_session.query(EventEmail).join(Event).filter(Event.entry == entry.id):
            orm_session.query(EventEmail).filter_by(event=email.event).delete(synchronize_session=False)
        for email in orm_session.query(ConditionEmail).join(Condition).filter(Condition.entry == entry.id):
            orm_session.query(ConditionEmail).filter_by(condition=email.condition).delete(synchronize_session=False)

    async def email_details(self, station: typing.Optional[str], entry_code: str, start_epoch: float,
                            result: typing.Callable[["DisplayInterface.EmailDetails"], None]):
        if station is None:
            station = ''
        start_date = datetime.datetime.fromtimestamp(int(start_epoch), tz=datetime.timezone.utc)
        end_date = datetime.datetime.now(tz=datetime.timezone.utc)
        end_date = end_date.replace(microsecond=0)

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                entry = orm_session.query(Entry).filter_by(station=station, code=entry_code).one_or_none()
                if entry is None:
                    return None

                email_data = orm_session.query(EntryEmail).filter_by(entry=entry.id).one_or_none()
                if email_data:
                    prior_update = email_data.updated_at_last_send.replace(tzinfo=datetime.timezone.utc)
                    prior_send = email_data.last_sent.replace(tzinfo=datetime.timezone.utc)
                    email_data.updated_at_last_send = entry.updated
                    email_data.last_sent = end_date
                else:
                    prior_update = None
                    prior_send = None
                    email_data = EntryEmail(entry=entry.id, updated_at_last_send=entry.updated, last_sent=end_date)
                    orm_session.add(email_data)

                context = self.EmailDetails(orm_session, start_date, entry, prior_update, prior_send)
                v = result(context)
                context._session = None

                self._clear_email_pending(orm_session, entry)

                try:
                    orm_session.commit()
                except:
                    _LOGGER.debug("Failed to commit email update", exc_info=True)
                    return None
                return v

        return await self.db.execute(execute)

    async def list_entries(self) -> typing.List[Entry]:
        def execute(engine: Engine) -> typing.List[Entry]:
            result: typing.List[Entry] = list()
            with Session(engine) as orm_session:
                for entry in orm_session.query(Entry):
                    result.append(entry)
            return result

        return await self.db.execute(execute)
