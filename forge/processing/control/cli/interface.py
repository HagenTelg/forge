import typing
import logging
import asyncio
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from forge.crypto import PublicKey
from ..database import Interface, AccessStation, AccessAcquisition


_LOGGER = logging.getLogger(__name__)


class ControlInterface(Interface):
    @staticmethod
    def _access_station(orm_session: Session, key: str, station: str) -> AccessStation:
        access = orm_session.query(AccessStation).filter_by(public_key=key, station=station).one_or_none()
        if access is None:
            access = AccessStation(public_key=key, station=station)
            orm_session.add(access)
            orm_session.flush()
        return access

    @staticmethod
    def _select_access(orm_session: Session, **kwargs):
        def prepare_like(raw):
            if '*' in raw:
                return raw.replace('*', '%')
            return f'%{raw}%'

        query = orm_session.query(AccessStation)
        if kwargs.get('acquisition') is not None:
            if kwargs['acquisition']:
                query = query.join(AccessAcquisition)
            else:
                query = query.outerjoin(AccessAcquisition, AccessStation.id == AccessAcquisition.access_station)
                query = query.filter(AccessAcquisition.access_station.is_(None))
        if kwargs.get('public_key'):
            query = query.filter(AccessStation.public_key == kwargs['public_key'])
        if kwargs.get('station'):
            query = query.filter(AccessStation.station.ilike(prepare_like(kwargs['station'].lower())))
        if kwargs.get('access'):
            query = query.filter(AccessStation.id == int(kwargs['access']))
        return query

    async def set_access(self, key: PublicKey, station: str,
                         revoke_existing: bool = True,
                         acquisition: typing.Optional[bool] = True) -> None:
        key = self.key_to_column(key)
        station = station.lower()

        def execute(engine: Engine):
            with Session(engine) as orm_session:
                if revoke_existing:
                    orm_session.query(AccessStation).filter_by(station=station).delete(synchronize_session='fetch')

                access = self._access_station(orm_session, key, station)

                def set_component(table, state: typing.Optional[bool]):
                    if state is None:
                        return None

                    target = orm_session.query(table).filter_by(access_station=access.id)
                    if state:
                        existing = target.one_or_none()
                        if not existing:
                            existing = table(access_station=access.id)
                            orm_session.add(existing)
                        return existing
                    else:
                        target.delete()
                        return None

                set_component(AccessAcquisition, acquisition)

                orm_session.commit()

        return await self.db.execute(execute)

    async def revoke_access(self, **kwargs) -> None:
        def execute(engine: Engine):
            with Session(engine) as orm_session:
                self._select_access(orm_session, **kwargs).delete(synchronize_session='fetch')
                orm_session.commit()

        return await self.db.execute(execute)

    async def list_access(self, **kwargs) -> typing.List[typing.Dict]:
        def execute(engine: Engine) -> typing.List[typing.Dict]:
            result: typing.List[typing.Dict] = list()
            with Session(engine) as orm_session:
                for access in self._select_access(orm_session, **kwargs):
                    def has_access(table) -> bool:
                        return orm_session.query(table).filter_by(access_station=access.id).one_or_none() is not None

                    result.append({
                        'id': access.id,
                        'public_key': access.public_key,
                        'station': access.station,
                        'acquisition': has_access(AccessAcquisition),
                    })

            return result

        return await self.db.execute(execute)
