import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, ozone_modes, met_modes


station_modes = detach(aerosol_modes, ozone_modes, met_modes)


station_modes['aerosol-raw'].remove('aerosol-raw-umacstatus')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
