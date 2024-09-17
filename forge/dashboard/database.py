import typing
import logging
import sqlalchemy as db
import sqlalchemy.orm as orm
from base64 import b64encode, b64decode
from forge.database import Database
from forge.crypto import PublicKey, key_to_bytes
from forge.dashboard import Severity


_LOGGER = logging.getLogger(__name__)
_Base = orm.declarative_base()


class Entry(_Base):
    __tablename__ = 'dashboard_entry'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    id = db.Column(db.Integer, primary_key=True)
    station = db.Column(db.String(32))
    code = db.Column(db.String(64))
    failed = db.Column(db.Boolean)
    updated = db.Column(db.DateTime)


db.Index('dashboard_entry_index', Entry.station, Entry.code, unique=True)


class EntryEmail(_Base):
    __tablename__ = 'entry_email'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    entry = db.Column(db.Integer, db.ForeignKey('dashboard_entry.id', ondelete='CASCADE'), primary_key=True)
    updated_at_last_send = db.Column(db.DateTime)
    last_sent = db.Column(db.DateTime)

    _entries = orm.relationship(Entry, backref=orm.backref('entry_email', passive_deletes=True))


class Notification(_Base):
    __tablename__ = 'entry_notification'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    id = db.Column(db.Integer, primary_key=True)
    entry = db.Column(db.Integer, db.ForeignKey('dashboard_entry.id', ondelete='CASCADE'), index=True)
    code = db.Column(db.String(64))
    severity = db.Column(db.Enum(Severity))
    data = db.Column(db.UnicodeText, nullable=True)

    _entries = orm.relationship(Entry, backref=orm.backref('entry_notification', passive_deletes=True))


db.Index('dashboard_notification_index', Notification.entry, Notification.code, unique=True)


class Watchdog(_Base):
    __tablename__ = 'entry_watchdog'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    id = db.Column(db.Integer, primary_key=True)
    entry = db.Column(db.Integer, db.ForeignKey('dashboard_entry.id', ondelete='CASCADE'), index=True)
    code = db.Column(db.String(64))
    severity = db.Column(db.Enum(Severity))
    last_seen = db.Column(db.DateTime, nullable=True)
    data = db.Column(db.UnicodeText, nullable=True)

    _entries = orm.relationship(Entry, backref=orm.backref('entry_watchdog', passive_deletes=True))


db.Index('dashboard_watchdog_index', Watchdog.entry, Watchdog.code, unique=True)


class Event(_Base):
    __tablename__ = 'event_history'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    id = db.Column(db.Integer, primary_key=True)
    entry = db.Column(db.Integer, db.ForeignKey('dashboard_entry.id', ondelete='CASCADE'), index=True)
    code = db.Column(db.String(64), index=True)
    severity = db.Column(db.Enum(Severity))
    occurred_at = db.Column(db.DateTime, index=True)
    data = db.Column(db.UnicodeText, nullable=True)

    _entries = orm.relationship(Entry, backref=orm.backref('event_history', passive_deletes=True))


class EventEmail(_Base):
    __tablename__ = 'event_email'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    event = db.Column(db.Integer, db.ForeignKey('event_history.id', ondelete='CASCADE'), primary_key=True)

    _events = orm.relationship(Event, backref=orm.backref('event_history', passive_deletes=True))


class Condition(_Base):
    __tablename__ = 'condition_history'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    id = db.Column(db.Integer, primary_key=True)
    entry = db.Column(db.Integer, db.ForeignKey('dashboard_entry.id', ondelete='CASCADE'), index=True)
    code = db.Column(db.String(64), index=True)
    severity = db.Column(db.Enum(Severity))
    start_time = db.Column(db.DateTime, index=True)
    end_time = db.Column(db.DateTime, index=True)
    data = db.Column(db.UnicodeText, nullable=True)

    _entries = orm.relationship(Entry, backref=orm.backref('condition_history', passive_deletes=True))


class ConditionEmail(_Base):
    __tablename__ = 'condition_email'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    condition = db.Column(db.Integer, db.ForeignKey('condition_history.id', ondelete='CASCADE'), primary_key=True)

    _conditions = orm.relationship(Condition, backref=orm.backref('condition_history', passive_deletes=True))


class AccessKey(_Base):
    __tablename__ = 'dashboard_access_key'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    id = db.Column(db.Integer, primary_key=True)
    public_key = db.Column(db.String(44), index=True)  # Base64 encoding
    station = db.Column(db.String(32))
    code = db.Column(db.String(64))


db.Index('dashboard_access_key_index_full',
         AccessKey.public_key, AccessKey.station, AccessKey.code, unique=True)


class AccessBearer(_Base):
    __tablename__ = 'dashboard_access_bearer'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    id = db.Column(db.Integer, primary_key=True)
    bearer_token = db.Column(db.String(44), index=True)  # 32 bytes of URL safe or base64
    station = db.Column(db.String(32))
    code = db.Column(db.String(64))


db.Index('dashboard_access_bearer_index_full',
         AccessBearer.bearer_token, AccessBearer.station, AccessBearer.code, unique=True)



class Interface:
    def __init__(self, uri: str):
        self.db = Database(uri, _Base)

    @staticmethod
    def key_to_column(key: PublicKey) -> str:
        return b64encode(key_to_bytes(key)).decode('ascii')

    @staticmethod
    def key_from_column(key: str) -> PublicKey:
        return PublicKey.from_public_bytes(b64decode(key))
