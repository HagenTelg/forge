import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes
from .acquisition import Acquisition


station_modes = detach(aerosol_modes)


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-clouds', "Clouds"),
                                    'aerosol-raw-temperature')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-clouds', "Clouds"),
                                         'aerosol-realtime-temperature')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-clouds', "Clouds"),
                                        'aerosol-editing-wind')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-clouds', "Clouds"),
                                      'aerosol-clean-wind')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-clean-clouds', "Clouds"),
                                     'aerosol-avgh-wind')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-cpcstatus2', "Second CPC Status"),
                                    'aerosol-raw-cpcstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-cpcstatus2', "Second CPC Status"),
                                         'aerosol-realtime-cpcstatus')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-hurricane', "Hurricane Hardened"),
                                    'aerosol-raw-umacstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-hurricane', "Hurricane Hardened"),
                                         'aerosol-realtime-umacstatus')

station_modes['acquisition'] = Acquisition()


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
