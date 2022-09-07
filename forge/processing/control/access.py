import typing
import logging
import asyncio
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from forge.crypto import PublicKey
from .database import Interface, AccessStation, AccessAcquisition, AccessData, AccessBackup, AccessAuxiliary


_LOGGER = logging.getLogger(__name__)


class AccessInterface(Interface):
    async def acquisition_uplink_authorized(self, key: PublicKey, station: str) -> bool:
        def execute(engine: Engine) -> bool:
            with Session(engine) as orm_session:
                query = orm_session.query(AccessAcquisition)
                query = query.join(AccessStation)
                query = query.filter(AccessStation.public_key == self.key_to_column(key))
                query = query.filter(AccessStation.station == station.lower())

                matched = query.one_or_none()

                return matched is not None

        return await self.db.execute(execute)

    async def incoming_data_authorized(self, key: PublicKey, station: str) -> bool:
        def execute(engine: Engine) -> bool:
            with Session(engine) as orm_session:
                query = orm_session.query(AccessData)
                query = query.join(AccessStation)
                query = query.filter(AccessStation.public_key == self.key_to_column(key))
                query = query.filter(AccessStation.station == station.lower())

                matched = query.one_or_none()

                return matched is not None

        return await self.db.execute(execute)

    async def incoming_backup_authorized(self, key: PublicKey, station: str) -> bool:
        def execute(engine: Engine) -> bool:
            with Session(engine) as orm_session:
                query = orm_session.query(AccessBackup)
                query = query.join(AccessStation)
                query = query.filter(AccessStation.public_key == self.key_to_column(key))
                query = query.filter(AccessStation.station == station.lower())

                matched = query.one_or_none()

                return matched is not None

        return await self.db.execute(execute)

    async def incoming_auxiliary_authorized(self, key: PublicKey, station: str) -> bool:
        def execute(engine: Engine) -> bool:
            with Session(engine) as orm_session:
                query = orm_session.query(AccessAuxiliary)
                query = query.join(AccessStation)
                query = query.filter(AccessStation.public_key == self.key_to_column(key))
                query = query.filter(AccessStation.station == station.lower())

                matched = query.one_or_none()

                return matched is not None

        return await self.db.execute(execute)
