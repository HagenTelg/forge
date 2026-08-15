import typing
from ..default.view import detach, View, aerosol_views, ozone_views, radiation_views
from ..default.aerosol.t640 import T640MassAethalometer, T640Status
from ..default.aerosol.editing.t640 import EditingT640
from ..default.ozone.teledynen500 import NOxConcentration, TeledyneN500Status
from ..default.ozone.editing.teledynen500 import EditingNOxConcentration


station_views = detach(aerosol_views, ozone_views, radiation_views)


station_views['aerosol-raw-mass'] = T640MassAethalometer('aerosol-raw')
station_views['aerosol-raw-t640status'] = T640Status('aerosol-raw')
station_views['aerosol-editing-mass'] = EditingT640('aerosol')
station_views['aerosol-clean-mass'] = T640MassAethalometer('aerosol-clean')
station_views['aerosol-avgh-mass'] = T640MassAethalometer('aerosol-avgh')
station_views['aerosol-realtime-mass'] = T640MassAethalometer('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-t640status'] = T640Status('aerosol-realtime', realtime=True)


station_views['ozone-raw-nox'] = NOxConcentration('ozone-raw')
station_views['ozone-realtime-nox'] = NOxConcentration('ozone-realtime', realtime=True)
station_views['ozone-editing-nox'] = EditingNOxConcentration()
station_views['ozone-clean-nox'] = NOxConcentration('ozone-clean')
station_views['ozone-avgh-nox'] = NOxConcentration('ozone-avgh')

station_views['ozone-raw-noxstatus'] = TeledyneN500Status('ozone-raw')
station_views['ozone-realtime-noxstatus'] = TeledyneN500Status('ozone-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
