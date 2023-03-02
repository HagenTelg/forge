import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, ozone_modes


station_modes = detach(aerosol_modes, ozone_modes)

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-mass', "Mass"),
                                    'aerosol-raw-aethalometer')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-mass', "Mass"),
                                         'aerosol-realtime-aethalometer')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-pops', "POPS"),
                                    'aerosol-raw-mass')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-pops', "POPS"),
                                         'aerosol-realtime-mass')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-pops', "POPS"),
                                        'aerosol-editing-absorption')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-pops', "POPS"),
                                      'aerosol-clean-aethalometer')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-pops', "POPS"),
                                     'aerosol-avgh-aethalometer')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-cpcstatus2', "MAGIC CPC Status"),
                                    'aerosol-raw-cpcstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-cpcstatus2', "MAGIC CPC Status"),
                                         'aerosol-realtime-cpcstatus')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-t640status', "T640 Status"),
                                    'aerosol-raw-cpcstatus2')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-popsstatus', "POPS Status"),
                                    'aerosol-raw-t640status')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-t640status', "T640 Status"),
                                         'aerosol-realtime-cpcstatus2')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-popsstatus', "POPS Status"),
                                         'aerosol-realtime-t640status')

station_modes['aerosol-realtime'].remove('aerosol-realtime-wind')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-opticalscattering2', "Ecotech Optical"),
                                    'aerosol-raw-optical')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-opticalscattering2', "Ecotech Optical"),
                                         'aerosol-realtime-optical')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-opticalscattering2', "Ecotech Optical"),
                                      'aerosol-clean-optical')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-opticalscattering2', "Ecotech Optical"),
                                     'aerosol-avgh-optical')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-nephelometerzero2', "Ecotech Nephelometer Zero"),
                                    'aerosol-raw-nephelometerstatus')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-nephelometerstatus2', "Ecotech Nephelometer Status"),
                                    'aerosol-raw-nephelometerzero2')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-nephelometerzero2', "Ecotech Nephelometer Zero"),
                                    'aerosol-realtime-nephelometerstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-nephelometerstatus2', "Ecotech Nephelometer Status"),
                                    'aerosol-realtime-nephelometerzero2')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-scattering2', "Ecotech Scattering"),
                                        'aerosol-editing-backscattering')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-backscattering2', "Ecotech Back Scattering"),
                                        'aerosol-editing-scattering2')

# station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-dmps', "DMPS"),
#                                     'aerosol-raw-mass')
# station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-dmps', "DMPS"),
#                                         'aerosol-editing-absorption')
# station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-dmps', "DMPS"),
#                                       'aerosol-clean-aethalometer')
# station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-dmps', "DMPS"),
#                                      'aerosol-avgh-aethalometer')
# station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-dmpsstatus', "DMPS Status"),
#                                     'aerosol-raw-t640status')



def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
