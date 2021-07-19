import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes


station_modes = detach(aerosol_modes)


station_modes['aerosol-raw'].remove('aerosol-raw-aethalometer')
station_modes['aerosol-raw'].remove('aerosol-raw-aethalometerstatus')
station_modes['aerosol-editing'].remove('aerosol-editing-aethalometer')
station_modes['aerosol-editing'].remove('aerosol-editing-aethalometerstatus')
station_modes['aerosol-clean'].remove('aerosol-clean-aethalometer')
station_modes['aerosol-avgh'].remove('aerosol-avgh-aethalometer')

station_modes['aerosol-raw'].remove('aerosol-raw-flow')
station_modes['aerosol-raw'].remove('aerosol-raw-umacstatus')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-smps', "SMPS"),
                                    'aerosol-raw-green')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-smps', "SMPS"),
                                        'aerosol-editing-absorption')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-smps', "SMPS"),
                                      'aerosol-clean-green')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-smps', "SMPS"),
                                     'aerosol-avgh-green')


station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-grimm', "Grimm OPC"),
                                    'aerosol-raw-smps')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-grimm', "Grimm OPC"),
                                        'aerosol-editing-smps')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-grimm', "Grimm OPC"),
                                      'aerosol-clean-smps')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-grimm', "Grimm OPC"),
                                     'aerosol-avgh-smps')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-grimmstatus', "Grimm OPC Status"),
                                    'aerosol-raw-ae33status')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
