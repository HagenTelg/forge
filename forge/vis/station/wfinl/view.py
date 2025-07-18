import typing
from ..default.view import  View
from ..default.aerosol.t640 import T640Mass, T640Status
from ..default.aerosol.editing.t640 import EditingT640


station_views = {}

station_views['aerosol-raw-mass'] = T640Mass('aerosol-raw')
station_views['aerosol-raw-t640status'] = T640Status('aerosol-raw')
station_views['aerosol-editing-mass'] = EditingT640('aerosol')
station_views['aerosol-clean-mass'] = T640Mass('aerosol-clean')
station_views['aerosol-avgh-mass'] = T640Mass('aerosol-avgh')
station_views['aerosol-realtime-mass'] = T640Mass('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-t640status'] = T640Status('aerosol-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
