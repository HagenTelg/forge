import typing
from ..default.view import detach, View, aerosol_views, met_views
from .purpleair import PurpleAir


station_views = detach(aerosol_views, met_views)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)


station_views['aerosol-raw-purpleair'] = PurpleAir('aerosol-raw')
station_views['aerosol-editing-purpleair'] = PurpleAir('aerosol-editing')
station_views['aerosol-clean-purpleair'] = PurpleAir('aerosol-clean')
station_views['aerosol-avgh-purpleair'] = PurpleAir('aerosol-avgh')
station_views['aerosol-realtime-purpleair'] = PurpleAir('aerosol-realtime', realtime=True)
