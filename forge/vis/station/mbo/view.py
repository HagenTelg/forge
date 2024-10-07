import typing
from ..default.view import detach, View, aerosol_views, aerosol_public, ozone_views
from ..default.aerosol.tsi3010cpc import TSI3010CPCStatus
from .counts import ParticleConcentration, EditingParticleConcentration, EditingGrimm
from .optical import OpticalCLAPSecondary, EditingCLAPSecondary, OpticalScatteringSecondary, EditingScatteringSecondary, EditingBackScatteringSecondary
from .ecotechnephelometer import NephelometerStatusSecondary, NephelometerZeroSecondary
from .clap import CLAPStatusSecondary
from .green import Green
from .grimm import GrimmDistribution, GrimmStatus
from .flow import Flow
from .gasses import Gasses
from .temperature import Temperature, Ambient
from .summary import Summary


station_views = detach(aerosol_views, aerosol_public, ozone_views)

station_views['aerosol-raw-counts'] = ParticleConcentration('aerosol-raw')
station_views['aerosol-realtime-counts'] = ParticleConcentration('aerosol-realtime', realtime=True)
station_views['aerosol-editing-counts'] = EditingParticleConcentration()
station_views['aerosol-clean-counts'] = ParticleConcentration('aerosol-clean')
station_views['aerosol-avgh-counts'] = ParticleConcentration('aerosol-avgh')
station_views['aerosol-raw-cpcstatus'] = TSI3010CPCStatus('aerosol-raw')
station_views['aerosol-realtime-cpcstatus'] = TSI3010CPCStatus('aerosol-realtime', realtime=True)

station_views['aerosol-raw-opticalclap2'] = OpticalCLAPSecondary('aerosol-raw')
station_views['aerosol-editing-clap2'] = EditingCLAPSecondary()
station_views['aerosol-clean-opticalclap2'] = OpticalCLAPSecondary('aerosol-clean')
station_views['aerosol-avgh-opticalclap2'] = OpticalCLAPSecondary('aerosol-avgh')
station_views['aerosol-realtime-opticalclap2'] = OpticalCLAPSecondary('aerosol-realtime', realtime=True)
station_views['aerosol-raw-clapstatus2'] = CLAPStatusSecondary('aerosol-raw')
station_views['aerosol-realtime-clapstatus2'] = CLAPStatusSecondary('aerosol-realtime', realtime=True)

station_views['aerosol-raw-opticalscattering2'] = OpticalScatteringSecondary('aerosol-raw')
station_views['aerosol-realtime-opticalscattering2'] = OpticalScatteringSecondary('aerosol-realtime', realtime=True)
station_views['aerosol-editing-scattering2'] = EditingScatteringSecondary()
station_views['aerosol-editing-backscattering2'] = EditingBackScatteringSecondary()
station_views['aerosol-clean-opticalscattering2'] = OpticalScatteringSecondary('aerosol-clean')
station_views['aerosol-avgh-opticalscattering2'] = OpticalScatteringSecondary('aerosol-avgh')
station_views['aerosol-raw-nephelometerzero2'] = NephelometerZeroSecondary('aerosol-raw')
station_views['aerosol-raw-nephelometerstatus2'] = NephelometerStatusSecondary('aerosol-raw')
station_views['aerosol-raw-nephelometerzero2'] = NephelometerZeroSecondary('aerosol-raw')
station_views['aerosol-raw-nephelometerstatus2'] = NephelometerStatusSecondary('aerosol-raw')
station_views['aerosol-realtime-nephelometerzero2'] = NephelometerZeroSecondary('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-nephelometerstatus2'] = NephelometerStatusSecondary('aerosol-realtime', realtime=True)

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


station_views['aerosol-raw-summary'] = Summary()


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
