import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, aerosol_public


station_modes = detach(aerosol_modes, aerosol_public)


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
