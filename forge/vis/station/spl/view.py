import typing
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.flow import BasicFlow
from ..default.aerosol.tsi3010cpc import TSI3010CPCStatus
from ..default.aerosol.ccn import CCNStatus
from .counts import ParticleConcentration, EditingParticleConcentration, TSI3776CPCStatus


station_views = detach(aerosol_views)

station_views['aerosol-raw-counts'] = ParticleConcentration('aerosol-raw')
station_views['aerosol-editing-counts'] = EditingParticleConcentration()
station_views['aerosol-clean-counts'] = ParticleConcentration('aerosol-clean')
station_views['aerosol-avgh-counts'] = ParticleConcentration('aerosol-avgh')
station_views['aerosol-raw-cpcstatus'] = TSI3010CPCStatus('aerosol-raw')
station_views['aerosol-raw-cpcstatus2'] = TSI3776CPCStatus('aerosol-raw')

station_views['aerosol-raw-flow'] = BasicFlow('aerosol-raw')

station_views['aerosol-raw-ccnstatus'] = CCNStatus('aerosol-raw')


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
