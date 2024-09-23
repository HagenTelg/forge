import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, ozone_modes


station_modes = detach(aerosol_modes, ozone_modes)

station_modes['aerosol-raw'].remove('aerosol-raw-aethalometer')
station_modes['aerosol-raw'].remove('aerosol-raw-aethalometerstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-aethalometer')
station_modes['aerosol-realtime'].remove('aerosol-realtime-aethalometerstatus')
station_modes['aerosol-editing'].remove('aerosol-editing-aethalometer')
station_modes['aerosol-clean'].remove('aerosol-clean-aethalometer')
station_modes['aerosol-avgh'].remove('aerosol-avgh-aethalometer')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-opticalclap2', "Second CLAP/TAP Optical"),
                                    'aerosol-raw-optical')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-opticalclap2', "Second CLAP/TAP Optical"),
                                         'aerosol-realtime-optical')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-clap2', "Second CLAP/TAP"),
                                        'aerosol-editing-absorption')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-opticalclap2', "Second CLAP/TAP Optical"),
                                      'aerosol-clean-optical')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-opticalclap2', "Second CLAP/TAP Optical"),
                                     'aerosol-avgh-optical')

station_modes['aerosol-raw']['aerosol-raw-clapstatus'].display_name = "CLAP/TAP Status"
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-clapstatus2', "Second CLAP/TAP Status"),
                                    'aerosol-raw-clapstatus')
station_modes['aerosol-realtime']['aerosol-realtime-clapstatus'].display_name = "CLAP/TAP Status"
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-clapstatus2', "Second CLAP/TAP Status"),
                                         'aerosol-realtime-clapstatus')

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


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-grimm', "Grimm OPC"),
                                    'aerosol-raw-green')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-grimm', "Grimm OPC"),
                                         'aerosol-realtime-green')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-grimm', "Grimm OPC"),
                                        'aerosol-editing-counts')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-grimmdistribution', "Grimm Size Distribution"),
                                        'aerosol-editing-clap')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-grimm', "Grimm OPC"),
                                      'aerosol-clean-green')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-grimm', "Grimm OPC"),
                                     'aerosol-avgh-green')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-grimmstatus', "Grimm OPC Status"),
                                    'aerosol-raw-cpcstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-grimmstatus', "Grimm OPC Status"),
                                         'aerosol-realtime-cpcstatus')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-gasses', "Gasses"),
                                    'aerosol-raw-grimm')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-ambient', "Met Temperature and RH"),
                                    'aerosol-raw-temperature')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-summary', "Summary"),
                                    'aerosol-raw-umacstatus')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
