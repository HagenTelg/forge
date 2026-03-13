import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, aerosol_public


station_modes = detach(aerosol_modes, aerosol_public)


station_modes['aerosol-raw'].remove('aerosol-raw-counts')
station_modes['aerosol-realtime'].remove('aerosol-realtime-counts')
station_modes['aerosol-editing'].remove('aerosol-editing-counts')
station_modes['aerosol-clean'].remove('aerosol-clean-counts')
station_modes['aerosol-avgh'].remove('aerosol-avgh-counts')
station_modes['aerosol-raw'].remove('aerosol-raw-cpcstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-cpcstatus')

station_modes['aerosol-raw'].insert(ViewList.Entry('aerosol-raw-psapstatus', "PSAP Status"),
                                    'aerosol-raw-clapstatus')
station_modes['aerosol-realtime'].insert(ViewList.Entry('aerosol-realtime-psapstatus', "PSAP Status"),
                                         'aerosol-realtime-clapstatus')

station_modes['aerosol-raw'].remove('aerosol-raw-clapstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-clapstatus')

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
