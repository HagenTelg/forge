import typing
from forge.vis.access import BaseAccessUser
from forge.dashboard.display import DisplayInterface
from forge.vis.station.lookup import station_data, default_data
from . import Record
from .entry import Entry
from .permissions import is_available


def get_record(station: typing.Optional[str], code: str) -> typing.Optional[Record]:
    if code.startswith('example-'):
        from .example import example_record
        return example_record

    if not station:
        return default_data('dashboard', 'record')(station, code)

    return station_data(station, 'dashboard', 'record')(station, code)


async def list_entries(db: DisplayInterface, user: BaseAccessUser) -> typing.List[Entry]:
    result: typing.List[Entry] = list()
    for entry in await db.list_entries():
        station = entry.station
        if not station:
            station = None
        record = get_record(station, entry.code)
        if not record:
            continue
        if not is_available(user, station, entry.code):
            continue
        converted = await record.entry(db=entry, station=station, code=entry.code)
        if not converted:
            continue
        result.append(converted)
    return result

