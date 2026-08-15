import typing
from ..default.mode import Mode, detach, aerosol_modes, ozone_modes, radiation_modes, ViewList


station_modes = detach(aerosol_modes, ozone_modes, radiation_modes)

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-mass', "Mass"),
                                    'aerosol-raw-aethalometer')
station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-t640status', "T640 Status"),
                                    'aerosol-raw-cpcstatus')

station_modes['ozone-raw'].insert(ViewList.Entry('ozone-raw-nox', "NOₓ"),
                                  'ozone-raw-concentration')
station_modes['ozone-realtime'].insert(ViewList.Entry('ozone-realtime-nox', "NOₓ"),
                                       'ozone-realtime-concentration')
station_modes['ozone-editing'].insert(ViewList.Entry('ozone-editing-nox', "NOₓ"),
                                      'ozone-editing-concentration')
station_modes['ozone-clean'].insert(ViewList.Entry('ozone-clean-nox', "NOₓ"),
                                    'ozone-clean-concentration')
station_modes['ozone-avgh'].insert(ViewList.Entry('ozone-avgh-nox', "NOₓ"),
                                   'ozone-avgh-concentration')

station_modes['ozone-raw'].insert(ViewList.Entry('ozone-raw-noxstatus', "NOₓ Status"),
                                  'ozone-raw-cells')
station_modes['ozone-realtime'].insert(ViewList.Entry('ozone-realtime-noxstatus', "NOₓ Status"),
                                       'ozone-realtime-cells')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
