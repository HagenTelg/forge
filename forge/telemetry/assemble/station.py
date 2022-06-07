import typing
import os


async def get_station() -> typing.Optional[str]:
    station = os.environ.get('CPD3STATION', None)
    if station:
        return station.upper()

    from forge.telemetry import CONFIGURATION
    station = CONFIGURATION.get("TELEMETRY.STATION", None)
    if station and isinstance(station, str):
        return station.upper()

    station = CONFIGURATION.get("ACQUISITION.STATION", None)
    if station and isinstance(station, str):
        return station.upper()

    return None
