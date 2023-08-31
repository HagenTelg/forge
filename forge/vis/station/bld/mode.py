import typing
from ..default.mode import detach, Mode, ozone_modes, radiation_modes


station_modes = detach(radiation_modes, ozone_modes)


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
