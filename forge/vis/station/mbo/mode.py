import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, ozone_modes


station_modes = detach(aerosol_modes, ozone_modes)

station_modes['aerosol-raw'].remove('aerosol-raw-aethalometer')
station_modes['aerosol-raw'].remove('aerosol-raw-aethalometerstatus')
station_modes['aerosol-editing'].remove('aerosol-editing-aethalometer')
station_modes['aerosol-editing'].remove('aerosol-editing-aethalometerstatus')
station_modes['aerosol-clean'].remove('aerosol-clean-aethalometer')
station_modes['aerosol-avgh'].remove('aerosol-avgh-aethalometer')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-opticaltap', "TAP Optical"),
                                    'aerosol-raw-optical')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-tap', "TAP Absorption"),
                                        'aerosol-editing-absorption')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-opticaltap', "TAP Optical"),
                                      'aerosol-clean-optical')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-opticaltap', "TAP Optical"),
                                     'aerosol-avgh-optical')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-tapstatus', "TAP Status"),
                                    'aerosol-raw-clapstatus')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-grimm', "Grimm OPC"),
                                    'aerosol-raw-green')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-grimm', "Grimm OPC"),
                                        'aerosol-editing-counts')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-grimmdistribution', "Grimm Size Distribution"),
                                        'aerosol-editing-tap')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-grimm', "Grimm OPC"),
                                      'aerosol-clean-green')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-grimm', "Grimm OPC"),
                                     'aerosol-avgh-green')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-grimmstatus', "Grimm OPC Status"),
                                    'aerosol-raw-cpcstatus')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-gasses', "Gasses"),
                                    'aerosol-raw-grimm')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-ambient', "Met Temperature and RH"),
                                    'aerosol-raw-temperature')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
