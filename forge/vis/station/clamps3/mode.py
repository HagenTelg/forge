import typing
from ..default.mode import Mode, ViewList, Public, detach, aerosol_modes, aerosol_public, ozone_modes


station_modes = {
    'aerosol-raw': ViewList('aerosol-raw', "Raw", [
        ViewList.Entry('aerosol-raw-mass', "Mass Concentration"),
        ViewList.Entry('aerosol-realtime-t640status', "T640 Status"),
    ]),
}


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
