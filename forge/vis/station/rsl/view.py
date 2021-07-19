import typing
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.pressure import BasicPressure
from ..default.aerosol.tsi377Xcpc import TSI3772CPCStatus
from .counts import EditingParticleConcentration
from .smps import SMPSDistribution, SMPSCounts


station_views = detach(aerosol_views)

station_views['aerosol-raw-counts'] = SMPSCounts('aerosol-raw')
station_views['aerosol-editing-counts'] = EditingParticleConcentration()
station_views['aerosol-clean-counts'] = SMPSCounts('aerosol-clean')
station_views['aerosol-avgh-counts'] = SMPSCounts('aerosol-avgh')
station_views['aerosol-raw-cpcstatus'] = TSI3772CPCStatus('aerosol-raw')

station_views['aerosol-raw-pressure'] = BasicPressure('aerosol-raw')

station_views['aerosol-raw-smps'] = SMPSDistribution('aerosol-raw')
station_views['aerosol-editing-smps'] = SMPSDistribution('aerosol-editing')
station_views['aerosol-clean-smps'] = SMPSDistribution('aerosol-clean')
station_views['aerosol-avgh-smps'] = SMPSDistribution('aerosol-avgh')


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
