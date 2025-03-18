import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, met_modes


station_modes = detach(aerosol_modes, met_modes)

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-purpleair', "Purple Air"),
                                    'aerosol-raw-umacstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-purpleair', "Purple Air"),
                                         'aerosol-realtime-umacstatus')
station_modes['aerosol-editing'].insert(ViewList.Entry('aerosol-editing-purpleair', "Purple Air"),
                                        'aerosol-editing-aethalometer')
station_modes['aerosol-clean'].insert(ViewList.Entry('aerosol-clean-purpleair', "Purple Air"),
                                      'aerosol-clean-wind')
station_modes['aerosol-avgh'].insert(ViewList.Entry('aerosol-avgh-purpleair', "Purple Air"),
                                     'aerosol-avgh-wind')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
