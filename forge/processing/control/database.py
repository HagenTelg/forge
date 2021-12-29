import typing
import asyncio
from base64 import b64encode
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


db.Index('access_station_index', 'public_key', 'station', unique=True)


class AccessAcquisition(_Base):
    __tablename__ = 'access_acquisition'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    access_station = db.Column(db.Integer, db.ForeignKey('access_station.id', ondelete='CASCADE'), primary_key=True)

    _station = orm.relationship(AccessStation, backref=orm.backref('access_acquisition', passive_deletes=True))


class Interface:
    @staticmethod
    def key_to_column(key: PublicKey) -> str:
        return b64encode(key_to_bytes(key)).decode('ascii')

    def __init__(self, uri: str):
        self.db = ORMDatabase(uri, _Base)
