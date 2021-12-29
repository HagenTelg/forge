import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, ozone_modes, met_modes


station_modes = detach(aerosol_modes, ozone_modes, met_modes)

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-filterstatus', "PMEL Filter Status"),
                                    'aerosol-raw-pressure')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-filterstatus', "PMEL Filter Status"),
                                         'aerosol-realtime-pressure')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
