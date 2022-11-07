import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, ozone_modes


station_modes = detach(aerosol_modes, ozone_modes)

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-mass', "Mass"),
                                    'aerosol-raw-aethalometer')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-mass', "Mass"),
                                         'aerosol-realtime-aethalometer')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-dmps', "DMPS"),
                                    'aerosol-raw-mass')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-dmps', "DMPS"),
                                        'aerosol-editing-absorption')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-dmps', "DMPS"),
                                      'aerosol-clean-aethalometer')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-dmps', "DMPS"),
                                     'aerosol-avgh-aethalometer')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-pops', "POPS"),
                                    'aerosol-raw-dmps')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-pops', "POPS"),
                                        'aerosol-editing-dmps')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-pops', "POPS"),
                                      'aerosol-clean-dmps')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-pops', "POPS"),
                                     'aerosol-avgh-dmps')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-pops', "POPS"),
                                         'aerosol-realtime-mass')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-cpcstatus2', "MAGIC CPC Status"),
                                    'aerosol-raw-cpcstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-cpcstatus2', "MAGIC CPC Status"),
                                         'aerosol-realtime-cpcstatus')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-t640status', "T640 Status"),
                                    'aerosol-raw-cpcstatus2')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-dmpsstatus', "DMPS Status"),
                                    'aerosol-raw-t640status')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-popsstatus', "POPS Status"),
                                    'aerosol-raw-dmpsstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-t640status', "T640 Status"),
                                         'aerosol-realtime-cpcstatus2')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-popsstatus', "POPS Status"),
                                         'aerosol-realtime-t640status')

station_modes['aerosol-realtime'].remove('aerosol-realtime-wind')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
