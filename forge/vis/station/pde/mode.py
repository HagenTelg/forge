import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes


station_modes = detach(aerosol_modes)


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-opticalclap2', "ACAS CLAP Optical"),
                                    'aerosol-raw-optical')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-opticalclap2', "ACAS CLAP Optical"),
                                         'aerosol-realtime-optical')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-clap2', "ACAS CLAP"),
                                        'aerosol-editing-absorption')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-opticalclap2', "ACAS CLAP Optical"),
                                      'aerosol-clean-optical')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-opticalclap2', "ACAS CLAP Optical"),
                                     'aerosol-avgh-optical')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-clapstatus2', "ACAS CLAP Status"),
                                    'aerosol-raw-clapstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-clapstatus2', "ACAS CLAP Status"),
                                         'aerosol-realtime-clapstatus')


station_modes['aerosol-raw'].remove('aerosol-raw-aethalometer')
station_modes['aerosol-raw'].remove('aerosol-raw-aethalometerstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-aethalometer')
station_modes['aerosol-realtime'].remove('aerosol-realtime-aethalometerstatus')
station_modes['aerosol-editing'].remove('aerosol-editing-aethalometer')
station_modes['aerosol-editing'].remove('aerosol-editing-aethalometerstatus')
station_modes['aerosol-clean'].remove('aerosol-clean-aethalometer')
station_modes['aerosol-avgh'].remove('aerosol-avgh-aethalometer')


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

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-hurricane', "Hurricane Hardened"),
                                    'aerosol-raw-umacstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-hurricane', "Hurricane Hardened"),
                                         'aerosol-realtime-umacstatus')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
