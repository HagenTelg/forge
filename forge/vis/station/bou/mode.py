import typing
from ..default.mode import Mode, detach, ozone_modes, ozone_public, radiation_modes


station_modes = detach(ozone_modes, ozone_public, radiation_modes)

station_modes.pop("public-ozoneshort", None)
station_modes.pop("public-ozonelong", None)


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
