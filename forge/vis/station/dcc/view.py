import typing
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.pressure import BasicPressure
from .ecotechnephelometer import NephelometerZero
from .psap import PSAPStatus

station_views = detach(aerosol_views)

station_views['aerosol-raw-pressure'] = BasicPressure('aerosol-raw')
station_views['aerosol-realtime-pressure'] = BasicPressure('aerosol-realtime', realtime=True)

station_views['aerosol-raw-nephelometerzero'] = NephelometerZero('aerosol-raw')
station_views['aerosol-realtime-nephelometerzero'] = NephelometerZero('aerosol-realtime', realtime=True)

station_views['aerosol-raw-psapstatus'] = PSAPStatus('aerosol-raw')
station_views['aerosol-realtime-psapstatus'] = PSAPStatus('aerosol-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
