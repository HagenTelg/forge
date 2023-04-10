import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes


station_modes = detach(aerosol_modes)


station_modes['aerosol-raw'].remove('aerosol-raw-counts')
station_modes['aerosol-realtime'].remove('aerosol-realtime-counts')
station_modes['aerosol-editing'].remove('aerosol-editing-counts')
station_modes['aerosol-clean'].remove('aerosol-clean-counts')
station_modes['aerosol-avgh'].remove('aerosol-avgh-counts')
station_modes['aerosol-raw'].remove('aerosol-raw-cpcstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-cpcstatus')

station_modes['aerosol-raw'].remove('aerosol-raw-aethalometer')
station_modes['aerosol-raw'].remove('aerosol-raw-aethalometerstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-aethalometer')
station_modes['aerosol-realtime'].remove('aerosol-realtime-aethalometerstatus')
station_modes['aerosol-editing'].remove('aerosol-editing-aethalometer')
station_modes['aerosol-clean'].remove('aerosol-clean-aethalometer')
station_modes['aerosol-avgh'].remove('aerosol-avgh-aethalometer')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-maap', "MAAP"),
                                    'aerosol-raw-optical')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-maap', "MAAP"),
                                         'aerosol-realtime-optical')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-maap', "MAAP"),
                                        'aerosol-editing-absorption')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-maap', "MAAP"),
                                      'aerosol-clean-optical')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-maap', "MAAP"),
                                     'aerosol-avgh-optical')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-maapstatus', "MAAP Status"),
                                    'aerosol-raw-clapstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-maapstatus', "MAAP Status"),
                                         'aerosol-realtime-clapstatus')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-opticalscattering2', "Ecotech Optical"),
                                    'aerosol-raw-maap')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-opticalscattering2', "Ecotech Optical"),
                                         'aerosol-realtime-maap')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-scattering2', "Ecotech Scattering"),
                                        'aerosol-editing-backscattering')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-backscattering2', "Ecotech Back Scattering"),
                                        'aerosol-editing-scattering2')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-opticalscattering2', "Ecotech Optical"),
                                      'aerosol-clean-maap')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-opticalscattering2', "Ecotech Optical"),
                                     'aerosol-avgh-maap')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-nephelometerzero2', "Ecotech Nephelometer Zero"),
                                    'aerosol-raw-nephelometerstatus')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-nephelometerstatus2', "Ecotech Nephelometer Status"),
                                    'aerosol-raw-nephelometerzero2')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-nephelometerzero2', "Ecotech Nephelometer Zero"),
                                         'aerosol-realtime-nephelometerstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-nephelometerstatus2', "Ecotech Nephelometer Status"),
                                         'aerosol-realtime-nephelometerzero2')

station_modes['aerosol-raw'].remove('aerosol-raw-clapstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-clapstatus')
station_modes['aerosol-editing'].remove('aerosol-editing-absorption')

station_modes['aerosol-raw'].remove('aerosol-raw-wind')
station_modes['aerosol-realtime'].remove('aerosol-realtime-wind')
station_modes['aerosol-editing'].remove('aerosol-editing-wind')
station_modes['aerosol-clean'].remove('aerosol-clean-wind')
station_modes['aerosol-avgh'].remove('aerosol-avgh-wind')

station_modes['aerosol-raw'].remove('aerosol-raw-flow')
station_modes['aerosol-raw'].remove('aerosol-raw-umacstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-flow')
station_modes['aerosol-realtime'].remove('aerosol-realtime-umacstatus')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
