import typing
from ..default.mode import Mode, detach, aerosol_modes, met_modes


station_modes = detach(aerosol_modes, met_modes)


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
