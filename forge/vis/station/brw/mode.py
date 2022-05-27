import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, ozone_modes, met_modes


station_modes = detach(aerosol_modes, ozone_modes, met_modes)

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-filterstatus', "PMEL Filter Status"),
                                    'aerosol-raw-pressure')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-filterstatus', "PMEL Filter Status"),
                                         'aerosol-realtime-pressure')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-filterstatus2', "SCRIPPS Filter Status"),
                                    'aerosol-raw-filterstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-filterstatus2', "SCRIPPS Filter Status"),
                                         'aerosol-realtime-filterstatus')


station_modes['met-raw'].insert(ViewList.Entry('met-raw-tower', "Tower dT"))
station_modes['met-editing'].insert(ViewList.Entry('met-editing-tower', "Tower dT"))


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
