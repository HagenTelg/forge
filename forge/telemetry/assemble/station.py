import typing
import os


async def get_station() -> typing.Optional[str]:
    station = os.environ.get('CPD3STATION', None)
    if station:
        return station.upper()

    return None
