import typing
from ..default.view import detach, View, aerosol_views, ozone_views
from ..default.aerosol.pops import POPSStatus
from .dmps import DMPSStatus


station_views = detach(aerosol_views, ozone_views)

station_views['aerosol-raw-dmpsstatus'] = DMPSStatus('aerosol-raw')
station_views['aerosol-raw-popsstatus'] = POPSStatus('aerosol-raw')


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
