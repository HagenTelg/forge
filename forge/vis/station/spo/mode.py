import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, ozone_modes, met_modes


station_modes = detach(aerosol_modes, ozone_modes, met_modes)

station_modes['met-raw'].insert(ViewList.Entry('met-raw-tower', "Tower dT"))
station_modes['met-editing'].insert(ViewList.Entry('met-editing-tower', "Tower dT"))


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
