import typing
from ..default.mode import Mode, ViewList, detach, aerosol_modes, ozone_modes, met_modes, radiation_modes


station_modes = detach(aerosol_modes, ozone_modes, met_modes, radiation_modes)


station_modes['aerosol-raw'].remove('aerosol-raw-umacstatus')
station_modes['aerosol-realtime'].remove('aerosol-realtime-umacstatus')

station_modes['aerosol-realtime'].remove('aerosol-realtime-wind')
station_modes['ozone-realtime'].remove('ozone-realtime-wind')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
