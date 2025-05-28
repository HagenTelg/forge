import typing
import logging
import asyncio
import time
import datetime
import ipaddress
import re
import sqlalchemy as db
import sqlalchemy.orm as orm
from base64 import b64encode, b64decode
from json import dumps as to_json
from json import loads as from_json
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from forge.database import Database
from forge.crypto import PublicKey, key_to_bytes


_LOGGER = logging.getLogger(__name__)
_Base = orm.declarative_base()


class _Host(_Base):
    __tablename__ = 'host_data'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    id = db.Column(db.Integer, primary_key=True)
    public_key = db.Column(db.String(44), index=True, unique=True)  # Base64 encoding
    station = db.Column(db.String(32), nullable=True)
    remote_host = db.Column(db.String(41), nullable=True)  # Possibly IPv6
    last_seen = db.Column(db.DateTime)


class _HostDirect(_Base):
    __tablename__ = 'host_direct'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    host_data = db.Column(db.Integer, db.ForeignKey('host_data.id', ondelete='CASCADE'), primary_key=True)
    sequence_number = db.Column(db.BigInteger)

    _host = orm.relationship(_Host, backref=orm.backref('host_direct', passive_deletes=True))


class _AccessStation(_Base):
    __tablename__ = 'access_station'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    id = db.Column(db.Integer, primary_key=True)
    public_key = db.Column(db.String(44), index=True)  # Base64 encoding
    station = db.Column(db.String(32), index=True)


class _Telemetry(_Base):
    __tablename__ = 'telemetry'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    host_data = db.Column(db.Integer, db.ForeignKey('host_data.id', ondelete='CASCADE'), primary_key=True)
    last_update = db.Column(db.DateTime)
    telemetry = db.Column(db.Text)

    _hosts = orm.relationship(_Host, backref=orm.backref('host_telemetry', passive_deletes=True))


class _TelemetryTimeOffset(_Base):
    __tablename__ = 'telemetry_timeoffset'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    host_data = db.Column(db.Integer, db.ForeignKey('host_data.id', ondelete='CASCADE'), primary_key=True)
    last_update = db.Column(db.DateTime)
    offset = db.Column(db.Integer)

    _hosts = orm.relationship(_Host, backref=orm.backref('host_telemetry_timeoffset', passive_deletes=True))


class _TelemetryAddress(_Base):
    __tablename__ = 'telemetry_address'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    host_data = db.Column(db.Integer, db.ForeignKey('host_data.id', ondelete='CASCADE'), primary_key=True)
    last_update = db.Column(db.DateTime)
    public_address = db.Column(db.String(41), nullable=True)
    local_address = db.Column(db.String(16), nullable=True)
    local_address6 = db.Column(db.String(41), nullable=True)

    _hosts = orm.relationship(_Host, backref=orm.backref('host_telemetry_address', passive_deletes=True))
    
    
class _TelemetryLogin(_Base):
    __tablename__ = 'telemetry_login'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    host_data = db.Column(db.Integer, db.ForeignKey('host_data.id', ondelete='CASCADE'), primary_key=True)
    last_update = db.Column(db.DateTime)
    name = db.Column(db.String(32))

    _hosts = orm.relationship(_Host, backref=orm.backref('host_telemetry_login', passive_deletes=True))


class _TelemetryLogKernel(_Base):
    __tablename__ = 'telemetry_log_kernel'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    host_data = db.Column(db.Integer, db.ForeignKey('host_data.id', ondelete='CASCADE'), primary_key=True)
    last_update = db.Column(db.DateTime)
    events = db.Column(db.Text)

    _hosts = orm.relationship(_Host, backref=orm.backref('host_telemetry_log_kernel', passive_deletes=True))


class _TelemetryLogAcquisition(_Base):
    __tablename__ = 'telemetry_log_acquisition'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mariadb_engine': 'InnoDB',
    }

    host_data = db.Column(db.Integer, db.ForeignKey('host_data.id', ondelete='CASCADE'), primary_key=True)
    last_update = db.Column(db.DateTime)
    events = db.Column(db.Text)

    _hosts = orm.relationship(_Host, backref=orm.backref('host_telemetry_log_acquisition', passive_deletes=True))


def _key_to_column(key: PublicKey) -> str:
    return b64encode(key_to_bytes(key)).decode('ascii')


_MATCH_STATION = re.compile(br'[A-Za-z][0-9A-Za-z_]{0,31}')


def _is_valid_station(station: str) -> bool:
    if not station:
        return False
    try:
        encoded = station.encode('ascii')
    except UnicodeEncodeError:
        return False
    return _MATCH_STATION.fullmatch(encoded) is not None


class Interface:
    def __init__(self, uri: str):
        self.db = Database(uri, _Base)

    @staticmethod
    def _update_host_telemetry(orm_session: Session, current_time: datetime.datetime, host,
                               table: _Base, **kwargs) -> None:
        target = orm_session.query(table).filter_by(host_data=host.id).one_or_none()
        if target is None:
            target = table(host_data=host.id, last_update=current_time, **kwargs)
            orm_session.add(target)
        else:
            target.last_update = current_time
            for prop, value in kwargs.items():
                setattr(target, prop, value)

    @staticmethod
    def _update_host_log(orm_session: Session, current_time: datetime.datetime, host,
                         table: _Base, events: typing.List[typing.Dict[str, typing.Any]],
                         replace=True) -> None:
        if not isinstance(events, list):
            return
        if len(events) == 0:
            return
        for e in events:
            if 'time' not in e:
                e['time'] = time.time()

        target = orm_session.query(table).filter_by(host_data=host.id).one_or_none()
        if target is None:
            events.sort(key=lambda e: e['time'])
            del events[:-100]
            target = table(host_data=host.id, last_update=current_time, events=to_json(events))
            orm_session.add(target)
        else:
            target.last_update = current_time
            if not replace:
                combined = from_json(target.events)
                combined.extend(events)
                combined.sort(key=lambda e: e['time'])
                del combined[:-100]
                events = combined
            else:
                events.sort(key=lambda e: e['time'])
                del events[:-100]
            target.events = to_json(events)

    def _dispatch_telemetry(self, orm_session: Session, current_time: datetime.datetime, host,
                            telemetry: typing.Dict[str, typing.Any],
                            merge=False,
                            received_time: typing.Optional[float] = None) -> None:
        telemetry.pop('sequence_number', None)
        telemetry.pop('station', None)

        try:
            remote_time = float(telemetry.pop('time'))
            if received_time is None:
                received_time = time.time()
            if received_time <= 0:
                time_offset = 0
            else:
                time_offset = round(remote_time - received_time)

            self._update_host_telemetry(orm_session, current_time, host, _TelemetryTimeOffset,
                                        offset=time_offset)
        except (KeyError, ValueError, TypeError):
            pass

        try:
            public_address = telemetry.pop('public_address', None)
            if public_address is not None:
                try:
                    public_address = str(ipaddress.ip_address(public_address))
                except ValueError:
                    public_address = None

            local_address = telemetry.pop('local_address', None)
            if local_address is not None:
                try:
                    local_address = str(ipaddress.IPv4Address(local_address))
                except ValueError:
                    local_address = None

            local_address6 = telemetry.pop('local_address6', None)
            if local_address6 is not None:
                try:
                    local_address6 = str(ipaddress.IPv6Address(local_address6))
                except ValueError:
                    local_address6 = None

            if public_address or local_address or local_address6:
                self._update_host_telemetry(orm_session, current_time, host, _TelemetryAddress,
                                            public_address=public_address,
                                            local_address=local_address,
                                            local_address6=local_address6)

        except (KeyError, ValueError, TypeError):
            pass

        try:
            name = str(telemetry.pop('login_user'))

            self._update_host_telemetry(orm_session, current_time, host, _TelemetryLogin,
                                        name=name)
        except (KeyError, ValueError, TypeError):
            pass

        self._update_host_log(orm_session,  current_time, host,
                              _TelemetryLogKernel, telemetry.pop('log_kernel', None))
        self._update_host_log(orm_session,  current_time, host,
                              _TelemetryLogAcquisition, telemetry.pop('log_acquisition', None))

        if len(telemetry) > 0:
            if not merge:
                try:
                    encoded = to_json(telemetry)
                except:
                    return
                self._update_host_telemetry(orm_session, current_time, host, _Telemetry,
                                            telemetry=encoded)
            else:
                target = orm_session.query(_Telemetry).filter_by(host_data=host.id).one_or_none()
                if target is None:
                    try:
                        encoded = to_json(telemetry)
                    except:
                        return
                    target = _Telemetry(host_data=host.id, last_update=current_time, telemetry=encoded)
                    orm_session.add(target)
                else:
                    target.last_update = current_time
                    try:
                        merged = from_json(target.telemetry)
                        for key, value in telemetry.items():
                            merged[key] = value
                        encoded = to_json(merged)
                    except:
                        return
                    target.telemetry = encoded

    @staticmethod
    def _key_to_host(orm_session: Session, key: PublicKey, current_time: datetime.datetime = None) -> _Host:
        key = _key_to_column(key)
        if current_time is None:
            current_time = datetime.datetime.now(tz=datetime.timezone.utc)
        host = orm_session.query(_Host).filter_by(public_key=key).one_or_none()
        if host is None:
            host = _Host(public_key=key, last_seen=current_time)
            orm_session.add(host)
            orm_session.flush()
        else:
            host.last_seen = current_time
        return host

    def _key_to_host_connected(self, orm_session: Session, key: PublicKey,
                               address: typing.Optional[str], station: typing.Optional[str],
                               current_time: datetime.datetime = None):
        host = self._key_to_host(orm_session, key, current_time)

        if address:
            host.remote_host = address
        if _is_valid_station(station):
            host.station = station

        return host

    async def direct_update(self, key: PublicKey, address: typing.Optional[str],
                            telemetry: typing.Dict[str, typing.Any],
                            received_time: typing.Optional[float] = None) -> bool:
        try:
            sequence_number = int(telemetry.get('sequence_number'))
        except (ValueError, TypeError):
            return False

        if address is None:
            address = telemetry.get('public_address')
            if address is not None:
                try:
                    address = str(ipaddress.ip_address(address))
                except ValueError:
                    address = None

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                current_time = datetime.datetime.now(tz=datetime.timezone.utc)
                host = self._key_to_host(orm_session, key, current_time)

                direct = orm_session.query(_HostDirect).filter_by(host_data=host.id).one_or_none()
                if direct is None:
                    direct = _HostDirect(host_data=host.id, sequence_number=sequence_number)
                    orm_session.add(direct)
                else:
                    if direct.sequence_number >= sequence_number:
                        _LOGGER.debug(f"Direct update duplication detected {direct.sequence_number} vs {sequence_number} on {key}")
                        return False
                    direct.sequence_number = sequence_number

                if address:
                    host.remote_host = address
                station = telemetry.get('station')
                if station is not None:
                    station = str(station).strip().lower()
                    if _is_valid_station(station):
                        host.station = station

                _LOGGER.debug(f"Performing direct update for {key} ({station}) from {address}")
                self._dispatch_telemetry(orm_session, current_time, host, telemetry, received_time=received_time)

                try:
                    orm_session.commit()
                except:
                    _LOGGER.debug("Direct telemetry update commit failed", exc_info=True)
                    return False
            return True

        return await self.db.execute(execute)

    async def connected_update(self, key: PublicKey, address: typing.Optional[str],
                               station: typing.Optional[str], telemetry: typing.Dict[str, typing.Any],
                               partial=False) -> bool:
        def execute(engine: Engine):
            with Session(engine) as orm_session:
                current_time = datetime.datetime.now(tz=datetime.timezone.utc)
                host = self._key_to_host_connected(orm_session, key, address, station, current_time=current_time)

                _LOGGER.debug(f"Performing connected update for {key} ({station}) from {address}")
                self._dispatch_telemetry(orm_session, current_time, host, telemetry, merge=partial)

                try:
                    orm_session.commit()
                except:
                    _LOGGER.debug("Connected telemetry update commit failed", exc_info=True)
                    return False
            return True

        return await self.db.execute(execute)

    async def ping_host(self, key: PublicKey, address: typing.Optional[str], station: typing.Optional[str]) -> None:
        def execute(engine: Engine):
            with Session(engine) as orm_session:
                self._key_to_host_connected(orm_session, key, address, station)

                try:
                    orm_session.commit()
                except:
                    pass

        return await self.db.execute(execute)

    async def append_log_kernel(self, key: PublicKey, address: typing.Optional[str], station: typing.Optional[str],
                                events: typing.List[typing.Dict[str, typing.Any]]) -> None:
        def execute(engine: Engine):
            with Session(engine) as orm_session:
                current_time = datetime.datetime.now(tz=datetime.timezone.utc)
                host = self._key_to_host_connected(orm_session, key, address, station, current_time=current_time)

                _LOGGER.debug(f"Adding {len(events)} kernel log events for {key} ({station}) from {address}")
                self._update_host_log(orm_session, current_time, host, _TelemetryLogKernel, events,
                                      replace=False)

                try:
                    orm_session.commit()
                except:
                    _LOGGER.debug("Connected telemetry kernel log commit failed", exc_info=True)

        return await self.db.execute(execute)

    async def append_log_acquisition(self, key: PublicKey, address: typing.Optional[str], station: typing.Optional[str],
                                     events: typing.List[typing.Dict[str, typing.Any]]) -> None:
        def execute(engine: Engine):
            with Session(engine) as orm_session:
                current_time = datetime.datetime.now(tz=datetime.timezone.utc)
                host = self._key_to_host_connected(orm_session, key, address, station, current_time=current_time)

                _LOGGER.debug(f"Adding {len(events)} acquisition log events for {key} ({station}) from {address}")
                self._update_host_log(orm_session, current_time, host, _TelemetryLogAcquisition, events,
                                      replace=False)

                try:
                    orm_session.commit()
                except:
                    _LOGGER.debug("Connected telemetry acquisition log commit failed", exc_info=True)

        return await self.db.execute(execute)

    async def tunnel_connection_authorized(self, from_key: PublicKey, target_key: PublicKey):
        from_key = _key_to_column(from_key)
        target_key = _key_to_column(target_key)

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                if orm_session.query(_AccessStation).filter_by(public_key=from_key, station='*').one_or_none():
                    return True

                query = orm_session.query(_AccessStation)
                query = query.join(_Host, _Host.station == _AccessStation.station)
                query = query.filter(_Host.public_key == target_key)
                query = query.filter(_AccessStation.public_key == from_key)
                if query.one_or_none():
                    return True
            return False

        return await self.db.execute(execute)

    async def tunnel_station_target(self, from_key: PublicKey, station: str) -> typing.Optional[PublicKey]:
        from_key = _key_to_column(from_key)
        station = station.lower()

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                if orm_session.query(_AccessStation).filter_by(public_key=from_key, station='*').one_or_none():
                    host = orm_session.query(_Host).filter_by(
                        station=station
                    ).order_by(_Host.last_seen.desc()).first()
                else:
                    query = orm_session.query(_Host)
                    query = query.join(_AccessStation, _AccessStation.station == _Host.station)
                    query = query.filter(_AccessStation.public_key == from_key)
                    query = query.filter(_Host.station == station)
                    query = query.order_by(_Host.last_seen.desc())
                    host = query.first()
                if host:
                    return PublicKey.from_public_bytes(b64decode(host.public_key))
            return None

        return await self.db.execute(execute)


class ControlInterface:
    def __init__(self, uri: str):
        self.db = Database(uri, _Base)

    @staticmethod
    def _select_hosts(orm_session: Session, **kwargs):
        def prepare_like(raw):
            if '*' in raw:
                return raw.replace('*', '%')
            return f'%{raw}%'

        def to_time(raw):
            if isinstance(raw, datetime.datetime):
                return raw
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            seconds = round(float(raw) * 86400)
            return now - datetime.timedelta(seconds=seconds)

        query = orm_session.query(_Host)
        if kwargs.get('public_key'):
            query = query.filter(_Host.public_key == kwargs['public_key'])
        if kwargs.get('station'):
            query = query.filter(_Host.station.ilike(prepare_like(kwargs['station'].lower())))
        if kwargs.get('host'):
            query = query.filter(_Host.id == int(kwargs['host']))
        if kwargs.get('remote_host'):
            query = query.filter(_Host.remote_host.ilike(prepare_like(kwargs['remote_host'])))
        if kwargs.get('before'):
            query = query.filter(_Host.last_seen <= to_time(kwargs['before']))
        if kwargs.get('after'):
            query = query.filter(_Host.last_seen >= to_time(kwargs['after']))
        return query

    @staticmethod
    def _select_access_station(orm_session: Session, **kwargs):
        def prepare_like(raw):
            if '*' in raw:
                return raw.replace('*', '%')
            return f'%{raw}%'

        query = orm_session.query(_AccessStation)
        if kwargs.get('public_key'):
            query = query.filter(_AccessStation.public_key == kwargs['public_key'])
        if kwargs.get('station'):
            query = query.filter(_AccessStation.station.ilike(prepare_like(kwargs['station'].lower())))
        return query

    async def tunnel_station_to_public_key(self, station: str) -> PublicKey:
        station = station.lower()

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                host = orm_session.query(_Host).filter_by(
                    station=station
                ).order_by(_Host.last_seen.desc()).first()
                if host:
                    return PublicKey.from_public_bytes(b64decode(host.public_key))
            return None

        return await self.db.execute(execute)

    async def list_hosts(self, **kwargs) -> typing.List[typing.Dict]:
        def execute(engine: Engine) -> typing.List[typing.Dict]:
            result: typing.List[typing.Dict] = list()
            with Session(engine) as orm_session:
                for host in self._select_hosts(orm_session, **kwargs):
                    result.append({
                        'id': host.id,
                        'public_key': host.public_key,
                        'station': host.station,
                        'remote_host': host.remote_host,
                        'last_seen': host.last_seen,
                    })

            return result

        return await self.db.execute(execute)

    async def host_details(self, **kwargs) -> typing.List[typing.Dict]:
        def execute(engine: Engine) -> typing.List[typing.Dict]:
            result: typing.List[typing.Dict] = list()
            with Session(engine) as orm_session:
                for host in self._select_hosts(orm_session, **kwargs):
                    details = orm_session.query(_Telemetry).filter_by(host_data=host.id).one_or_none()
                    if details:
                        data = from_json(details.telemetry)
                    else:
                        data = {}
                    data['last_update'] = {}
                    if details:
                        data['last_update']['telemetry'] = details.last_update

                    data['id'] = host.id
                    data['public_key'] = host.public_key
                    data['station'] = host.station
                    data['remote_host'] = host.remote_host
                    data['last_seen'] = host.last_seen

                    add = orm_session.query(_TelemetryTimeOffset).filter_by(host_data=host.id).one_or_none()
                    if add:
                        data['last_update']['time_offset'] = add.last_update
                        data['time_offset'] = add.offset

                    add = orm_session.query(_TelemetryAddress).filter_by(host_data=host.id).one_or_none()
                    if add:
                        data['last_update']['address'] = add.last_update
                        data['public_address'] = add.public_address
                        data['local_address'] = add.local_address
                        data['local_address6'] = add.local_address6

                    add = orm_session.query(_TelemetryLogin).filter_by(host_data=host.id).one_or_none()
                    if add:
                        data['last_update']['login'] = add.last_update
                        data['login_user'] = add.name

                    add = orm_session.query(_TelemetryLogKernel).filter_by(host_data=host.id).one_or_none()
                    if add:
                        data['last_update']['log_kernel'] = add.last_update
                        data['log_kernel'] = from_json(add.events)

                    add = orm_session.query(_TelemetryLogAcquisition).filter_by(host_data=host.id).one_or_none()
                    if add:
                        data['last_update']['log_acquisition'] = add.last_update
                        data['log_acquisition'] = from_json(add.events)

                    result.append(data)

            return result

        return await self.db.execute(execute)

    async def login_info(self, **kwargs) -> typing.List[typing.Dict]:
        def execute(engine: Engine) -> typing.List[typing.Dict]:
            result: typing.List[typing.Dict] = list()
            with Session(engine) as orm_session:
                for host in self._select_hosts(orm_session, **kwargs):
                    data = {
                        'id': host.id,
                        'public_key': host.public_key,
                        'station': host.station,
                        'remote_host': host.remote_host,
                        'last_seen': host.last_seen,
                    }

                    add = orm_session.query(_TelemetryAddress).filter_by(host_data=host.id).one_or_none()
                    if add:
                        data['public_address'] = add.public_address
                        data['local_address'] = add.local_address
                        data['local_address6'] = add.local_address6

                    add = orm_session.query(_TelemetryLogin).filter_by(host_data=host.id).one_or_none()
                    if add:
                        data['login_user'] = add.name
                    else:
                        details = orm_session.query(_Telemetry).filter_by(host_data=host.id).one_or_none()
                        if details:
                            details = from_json(details.telemetry)
                            try:
                                from .assemble.login import convert_login_user
                                user = convert_login_user(details['users'])
                                if user:
                                    data['login_user'] = user
                            except (KeyError, ValueError, TypeError):
                                pass

                    result.append(data)

            return result

        return await self.db.execute(execute)

    async def purge_hosts(self, **kwargs) -> None:
        def execute(engine: Engine) -> None:
            with Session(engine) as orm_session:
                self._select_hosts(orm_session, **kwargs).delete(synchronize_session=False)
                orm_session.commit()

        return await self.db.execute(execute)

    async def list_access(self, **kwargs) -> typing.List[typing.Dict]:
        def execute(engine: Engine) -> typing.List[typing.Dict]:
            result: typing.List[typing.Dict] = list()
            with Session(engine) as orm_session:
                for access in self._select_access_station(orm_session, **kwargs):
                    result.append({
                        'id': access.id,
                        'public_key': access.public_key,
                        'station': access.station,
                    })

            return result

        return await self.db.execute(execute)

    async def grant_station_access(self, public_key: PublicKey, stations: typing.List[str]) -> None:
        public_key = _key_to_column(public_key)

        def execute(engine: Engine) -> None:
            with Session(engine) as orm_session:
                for station in stations:
                    station = station.lower()
                    if orm_session.query(_AccessStation).filter_by(public_key=public_key,
                                                                   station=station).one_or_none():
                        _LOGGER.debug(
                            f"Skipping already granted station access for {public_key} on {station}")
                        continue

                    orm_session.add(_AccessStation(public_key=public_key, station=station))
                    _LOGGER.info(f"Adding station access for {public_key} on {station}")

                orm_session.commit()

        return await self.db.execute(execute)

    async def access_revoke(self, **kwargs) -> None:
        def execute(engine: Engine) -> None:
            with Session(engine) as orm_session:
                self._select_access_station(orm_session, **kwargs).delete(synchronize_session=False)
                orm_session.commit()

        return await self.db.execute(execute)


class DisplayInterface:
    def __init__(self, uri: str):
        self.db = Database(uri, _Base)

    async def list_stations(self) -> typing.Set[str]:
        def execute(engine: Engine) -> typing.Set[str]:
            with Session(engine) as orm_session:
                result: typing.Set[str] = set()
                for v in orm_session.query(_Host.station.distinct()):
                    result.add(v[0].lower())
                return result

        return await self.db.execute(execute)

    @staticmethod
    def _filter_keys(query, accepted_keys: typing.Optional[typing.List[PublicKey]]):
        if not accepted_keys:
            return query
        query = query.filter(db.or_(
            *[_Host.public_key == _key_to_column(k) for k in accepted_keys]
        ))
        return query

    async def get_last_seen(self, station: str,
                            accepted_keys: typing.Optional[typing.List[PublicKey]] = None) -> typing.Optional[datetime.datetime]:
        station = station.lower()

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                query = orm_session.query(_Host).filter_by(station=station)
                query = self._filter_keys(query, accepted_keys)
                query = query.order_by(_Host.last_seen.desc())
                host = query.first()
                if host:
                    return host.last_seen
            return None

        return await self.db.execute(execute)

    async def get_status(self, station: str,
                         accepted_keys: typing.Optional[typing.List[PublicKey]] = None) -> typing.Tuple[typing.Optional[datetime.datetime], typing.Optional[int]]:
        station = station.lower()

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                query = orm_session.query(_Host).filter_by(station=station)
                query = self._filter_keys(query, accepted_keys)
                query = query.order_by(_Host.last_seen.desc())
                host = query.first()
                if not host:
                    return None, None

                time_offset = orm_session.query(_TelemetryTimeOffset).filter_by(host_data=host.id).first()
                if time_offset:
                    time_offset = time_offset.offset
                else:
                    time_offset = None

                return host.last_seen, time_offset

        return await self.db.execute(execute)

    async def get_time_offset(self, station: str,
                              accepted_keys: typing.Optional[typing.List[PublicKey]] = None) -> typing.Optional[int]:
        station = station.lower()

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                query = orm_session.query(_TelemetryTimeOffset)
                query = query.join(_Host)
                query = query.filter(_Host.station == station)
                query = self._filter_keys(query, accepted_keys)
                query = query.order_by(_Host.last_seen.desc())
                offset = query.first()
                if not offset:
                    return None
                return offset.offset

        return await self.db.execute(execute)
