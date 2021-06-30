import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, ozone_modes


station_modes = detach(aerosol_modes, ozone_modes)

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-dmps', "DMPS"),
                                    'aerosol-raw-aethalometer')
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

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-dmpsstatus', "DMPS Status"),
                                    'aerosol-raw-cpcstatus')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-popsstatus', "POPS Status"),
                                    'aerosol-raw-dmpsstatus')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
