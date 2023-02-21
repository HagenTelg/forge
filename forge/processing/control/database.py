import typing
import asyncio
from base64 import b64encode, b64decode
import sqlalchemy as db
import sqlalchemy.orm as orm
from forge.crypto import PublicKey, key_to_bytes
from forge.database import ORMDatabase


_Base = orm.declarative_base()


class AccessStation(_Base):
    __tablename__ = 'access_station'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    id = db.Column(db.Integer, primary_key=True)
    public_key = db.Column(db.String(44))  # Base64 encoding
    station = db.Column(db.String(32))


db.Index('access_station_index', AccessStation.public_key, AccessStation.station, unique=True)


class AccessAcquisition(_Base):
    __tablename__ = 'access_acquisition'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    access_station = db.Column(db.Integer, db.ForeignKey('access_station.id', ondelete='CASCADE'), primary_key=True)

    _station = orm.relationship(AccessStation, backref=orm.backref('access_acquisition', passive_deletes=True))


class AccessData(_Base):
    __tablename__ = 'access_data'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    access_station = db.Column(db.Integer, db.ForeignKey('access_station.id', ondelete='CASCADE'), primary_key=True)

    _station = orm.relationship(AccessStation, backref=orm.backref('access_data', passive_deletes=True))


class AccessBackup(_Base):
    __tablename__ = 'access_backup'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    access_station = db.Column(db.Integer, db.ForeignKey('access_station.id', ondelete='CASCADE'), primary_key=True)

    _station = orm.relationship(AccessStation, backref=orm.backref('access_backup', passive_deletes=True))


class AccessAuxiliary(_Base):
    __tablename__ = 'access_auxiliary'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    access_station = db.Column(db.Integer, db.ForeignKey('access_station.id', ondelete='CASCADE'), primary_key=True)

    _station = orm.relationship(AccessStation, backref=orm.backref('access_auxiliary', passive_deletes=True))


class Interface:
    @staticmethod
    def key_to_column(key: PublicKey) -> str:
        return b64encode(key_to_bytes(key)).decode('ascii')

    @staticmethod
    def key_from_column(key: str) -> PublicKey:
        return PublicKey.from_public_bytes(b64decode(key))

    def __init__(self, uri: str):
        self.db = ORMDatabase(uri, _Base)
