import typing
from ..default.view import detach, View, aerosol_views, ozone_views, met_views, radiation_views
from ..default.ozone.concentration import OzoneConcentration
from ..default.ozone.teledynen500 import NOxConcentration, TeledyneN500Status
from ..default.ozone.editing.teledynen500 import EditingNOxConcentration


station_views = detach(aerosol_views, ozone_views, met_views, radiation_views)

station_views['public-realtime-ozone-concentration'] = OzoneConcentration('public-realtime', realtime=True)

station_views['ozone-raw-nox'] = NOxConcentration('ozone-raw')
station_views['ozone-realtime-nox'] = NOxConcentration('ozone-realtime', realtime=True)
station_views['ozone-editing-nox'] = EditingNOxConcentration()
station_views['ozone-clean-nox'] = NOxConcentration('ozone-clean')
station_views['ozone-avgh-nox'] = NOxConcentration('ozone-avgh')

station_views['ozone-raw-noxstatus'] = TeledyneN500Status('ozone-raw')
station_views['ozone-realtime-noxstatus'] = TeledyneN500Status('ozone-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
