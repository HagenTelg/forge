import typing
from ..default.mode import Mode, detach, aerosol_modes


station_modes = detach(aerosol_modes)


station_modes['aerosol-raw'].remove('aerosol-raw-counts')
station_modes['aerosol-realtime'].remove('aerosol-realtime-counts')
station_modes['aerosol-editing'].remove('aerosol-editing-counts')
station_modes['aerosol-clean'].remove('aerosol-clean-counts')
station_modes['aerosol-avgh'].remove('aerosol-avgh-counts')
station_modes['aerosol-raw'].remove('aerosol-raw-cpcstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-cpcstatus')

station_modes['aerosol-raw'].remove('aerosol-raw-aethalometer')
station_modes['aerosol-raw'].remove('aerosol-raw-aethalometerstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-aethalometer')
station_modes['aerosol-realtime'].remove('aerosol-realtime-aethalometerstatus')
station_modes['aerosol-editing'].remove('aerosol-editing-aethalometer')
station_modes['aerosol-clean'].remove('aerosol-clean-aethalometer')
station_modes['aerosol-avgh'].remove('aerosol-avgh-aethalometer')

station_modes['aerosol-raw'].remove('aerosol-raw-flow')
station_modes['aerosol-raw'].remove('aerosol-raw-umacstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-flow')
station_modes['aerosol-realtime'].remove('aerosol-realtime-umacstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-wind')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
