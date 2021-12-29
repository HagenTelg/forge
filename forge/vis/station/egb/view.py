import typing
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.pressure import BasicPressure
from ..default.aerosol.tsi377Xcpc import TSI3775CPCStatus
from .counts import RealtimeParticleConcentration, EditingParticleConcentration
from .smps import SMPSDistribution, SMPSCounts
from .grimm import GrimmDistribution, GrimmStatus


station_views = detach(aerosol_views)

station_views['aerosol-raw-counts'] = SMPSCounts('aerosol-raw')
station_views['aerosol-realtime-counts'] = RealtimeParticleConcentration('aerosol-realtime')
station_views['aerosol-editing-counts'] = EditingParticleConcentration()
station_views['aerosol-clean-counts'] = SMPSCounts('aerosol-clean')
station_views['aerosol-avgh-counts'] = SMPSCounts('aerosol-avgh')

station_views['aerosol-raw-cpcstatus'] = TSI3775CPCStatus('aerosol-raw')
station_views['aerosol-realtime-cpcstatus'] = TSI3775CPCStatus('aerosol-realtime', realtime=True)

station_views['aerosol-raw-pressure'] = BasicPressure('aerosol-raw')
station_views['aerosol-realtime-pressure'] = BasicPressure('aerosol-realtime', realtime=True)

station_views['aerosol-raw-smps'] = SMPSDistribution('aerosol-raw')
station_views['aerosol-editing-smps'] = SMPSDistribution('aerosol-editing')
station_views['aerosol-clean-smps'] = SMPSDistribution('aerosol-clean')
station_views['aerosol-avgh-smps'] = SMPSDistribution('aerosol-avgh')

station_views['aerosol-raw-grimm'] = GrimmDistribution('aerosol-raw')
station_views['aerosol-raw-grimmstatus'] = GrimmStatus('aerosol-raw')
station_views['aerosol-realtime-grimm'] = GrimmDistribution('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-grimmstatus'] = GrimmStatus('aerosol-realtime', realtime=True)
station_views['aerosol-editing-grimm'] = GrimmDistribution('aerosol-editing')
station_views['aerosol-clean-grimm'] = GrimmDistribution('aerosol-clean')
station_views['aerosol-avgh-grimm'] = GrimmDistribution('aerosol-avgh')


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
