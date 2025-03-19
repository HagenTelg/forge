import typing
from ..default.mode import Mode, detach, ozone_modes, ozone_public, aerosol_modes, aerosol_public

station_modes = detach(ozone_modes, ozone_public, aerosol_modes, aerosol_public)


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
