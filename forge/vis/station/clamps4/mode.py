import typing
from ..default.mode import Mode, ViewList, Realtime, DefaultAcquisition


station_modes = {
    'aerosol-raw': ViewList('aerosol-raw', "Raw", [
        ViewList.Entry('aerosol-raw-mass', "Mass Concentration"),
        ViewList.Entry('aerosol-raw-t640status', "T640 Status"),
    ]),
    'aerosol-realtime': Realtime('aerosol-realtime', "Realtime", [
        ViewList.Entry('aerosol-realtime-mass', "Mass Concentration"),
        ViewList.Entry('aerosol-realtime-t640status', "T640 Status"),
    ]),
    'acquisition': DefaultAcquisition(),
}

def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
