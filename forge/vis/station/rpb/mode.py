import typing
from ..default.mode import Mode, detach, ozone_modes, aerosol_modes

station_modes = detach(ozone_modes, aerosol_modes)


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
