import typing
from ..default.view import detach, View, aerosol_views, ozone_views
from ..default.aerosol.tsi3010cpc import TSI3010CPCStatus
from .counts import ParticleConcentration, EditingParticleConcentration, EditingGrimm
from .optical import OpticalTAP, EditingTAP
from .tap import TAPStatus
from .green import Green
from .grimm import GrimmDistribution, GrimmStatus
from .flow import Flow
from .gasses import Gasses
from .temperature import Temperature, Ambient


station_views = detach(aerosol_views, ozone_views)

station_views['aerosol-raw-counts'] = ParticleConcentration('aerosol-raw')
station_views['aerosol-realtime-counts'] = ParticleConcentration('aerosol-realtime', realtime=True)
station_views['aerosol-editing-counts'] = EditingParticleConcentration()
station_views['aerosol-clean-counts'] = ParticleConcentration('aerosol-clean')
station_views['aerosol-avgh-counts'] = ParticleConcentration('aerosol-avgh')
station_views['aerosol-raw-cpcstatus'] = TSI3010CPCStatus('aerosol-raw')
station_views['aerosol-realtime-cpcstatus'] = TSI3010CPCStatus('aerosol-realtime', realtime=True)

station_views['aerosol-raw-opticaltap'] = OpticalTAP('aerosol-raw')
station_views['aerosol-realtime-opticaltap'] = OpticalTAP('aerosol-realtime', realtime=True)
station_views['aerosol-editing-tap'] = EditingTAP()
station_views['aerosol-clean-opticaltap'] = OpticalTAP('aerosol-clean')
station_views['aerosol-avgh-opticaltap'] = OpticalTAP('aerosol-avgh')
station_views['aerosol-raw-tapstatus'] = TAPStatus('aerosol-raw')
station_views['aerosol-realtime-tapstatus'] = TAPStatus('aerosol-realtime', realtime=True)

station_views['aerosol-raw-green'] = Green('aerosol-raw')
station_views['aerosol-realtime-green'] = Green('aerosol-realtime', realtime=True)
station_views['aerosol-clean-green'] = Green('aerosol-clean')
station_views['aerosol-avgh-green'] = Green('aerosol-avgh')

station_views['aerosol-raw-grimm'] = GrimmDistribution('aerosol-raw')
station_views['aerosol-raw-grimmstatus'] = GrimmStatus('aerosol-raw')
station_views['aerosol-realtime-grimm'] = GrimmDistribution('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-grimmstatus'] = GrimmStatus('aerosol-realtime', realtime=True)
station_views['aerosol-editing-grimm'] = EditingGrimm()
station_views['aerosol-editing-grimmdistribution'] = GrimmDistribution('aerosol-editing')
station_views['aerosol-clean-grimm'] = GrimmDistribution('aerosol-clean')
station_views['aerosol-avgh-grimm'] = GrimmDistribution('aerosol-avgh')

station_views['aerosol-raw-gasses'] = Gasses('aerosol-raw')
station_views['aerosol-raw-flow'] = Flow('aerosol-raw')
station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw')
station_views['aerosol-realtime-temperature'] = Temperature('aerosol-realtime', realtime=True)
station_views['aerosol-raw-ambient'] = Ambient('aerosol-raw')


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
