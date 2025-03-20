import typing
from ..default.mode import detach, Mode, ozone_modes, ozone_public, radiation_modes, ViewList


station_modes = detach(radiation_modes, ozone_modes, ozone_public)


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
