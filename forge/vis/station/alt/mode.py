import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes


station_modes = detach(aerosol_modes)


station_modes['aerosol-raw'].remove('aerosol-raw-flow')
station_modes['aerosol-raw'].remove('aerosol-raw-umacstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-flow')
station_modes['aerosol-realtime'].remove('aerosol-realtime-umacstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-wind')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-opticalclap2', "Second CLAP Optical"),
                                    'aerosol-raw-optical')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-opticalclap2', "Second CLAP Optical"),
                                         'aerosol-realtime-optical')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-clap2', "Second CLAP"),
                                        'aerosol-editing-absorption')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-opticalclap2', "Second CLAP Optical"),
                                      'aerosol-clean-optical')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-opticalclap2', "Second CLAP Optical"),
                                     'aerosol-avgh-optical')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-clapstatus2', "Second CLAP Status"),
                                    'aerosol-raw-clapstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-clapstatus2', "Second CLAP Status"),
                                         'aerosol-realtime-clapstatus')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-opticalpsap', "PSAP Optical"),
                                    'aerosol-raw-opticalclap2')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-opticalpsap', "PSAP Optical"),
                                         'aerosol-realtime-opticalclap2')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-psap', "PSAP"),
                                        'aerosol-editing-clap2')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-opticalpsap', "PSAP Optical"),
                                      'aerosol-clean-opticalclap2')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-opticalpsap', "PSAP Optical"),
                                     'aerosol-avgh-opticalclap2')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-psapstatus', "PSAP Status"),
                                    'aerosol-raw-clapstatus2')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-psapstatus', "PSAP Status"),
                                         'aerosol-realtime-clapstatus2')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-cpcstatus2', "Second CPC Status"),
                                    'aerosol-raw-cpcstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-cpcstatus2', "Second CPC Status"),
                                         'aerosol-realtime-cpcstatus')


station_modes['aerosol-raw'].remove('aerosol-raw-aethalometer')
station_modes['aerosol-raw'].remove('aerosol-raw-aethalometerstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-aethalometer')
station_modes['aerosol-realtime'].remove('aerosol-realtime-aethalometerstatus')
station_modes['aerosol-editing'].remove('aerosol-editing-aethalometer')
station_modes['aerosol-editing'].remove('aerosol-editing-aethalometerstatus')
station_modes['aerosol-clean'].remove('aerosol-clean-aethalometer')
station_modes['aerosol-avgh'].remove('aerosol-avgh-aethalometer')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-ae31', "AE31"),
                                    'aerosol-raw-green')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-ae31status', "AE31 Status"),
                                    'aerosol-raw-clapstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-ae31', "AE31"),
                                         'aerosol-realtime-green')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-ae31status', "AE31 Status"),
                                         'aerosol-realtime-clapstatus')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-ae31', "AE31"),
                                        'aerosol-editing-absorption')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-ae31status', "AE31 Status"),
                                        'aerosol-editing-ae31')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-ae31', "AE31"),
                                      'aerosol-clean-green')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-ae31', "AE31"),
                                     'aerosol-avgh-green')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-ae33', "AE33"),
                                    'aerosol-raw-ae31')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-ae33status', "AE33 Status"),
                                    'aerosol-raw-ae31status')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-ae33', "AE33"),
                                         'aerosol-realtime-ae31')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-ae33status', "AE33 Status"),
                                         'aerosol-realtime-ae31status')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-ae33', "AE33"),
                                        'aerosol-editing-ae31status')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-ae33status', "AE33 Status"),
                                        'aerosol-editing-ae33')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-ae33', "AE33"),
                                      'aerosol-clean-ae31')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-ae33', "AE33"),
                                     'aerosol-avgh-ae31')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-smps', "SMPS"),
                                    'aerosol-raw-ae33')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-smps', "SMPS"),
                                        'aerosol-editing-ae33status')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-smps', "SMPS"),
                                      'aerosol-clean-ae33')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-smps', "SMPS"),
                                     'aerosol-avgh-ae33')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-grimm', "Grimm OPC"),
                                    'aerosol-raw-smps')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-grimm', "Grimm OPC"),
                                         'aerosol-realtime-smps')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-grimm', "Grimm OPC"),
                                        'aerosol-editing-smps')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-grimm', "Grimm OPC"),
                                      'aerosol-clean-smps')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-grimm', "Grimm OPC"),
                                     'aerosol-avgh-smps')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-grimmstatus', "Grimm OPC Status"),
                                    'aerosol-raw-ae33status')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-grimmstatus', "Grimm OPC Status"),
                                         'aerosol-realtime-ae33status')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
