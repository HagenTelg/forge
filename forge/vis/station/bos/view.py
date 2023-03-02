import typing
from ..default.view import detach, View, aerosol_views, ozone_views
from ..default.aerosol.pops import POPSStatus, POPSDistribution
from ..default.aerosol.t640 import T640MassAethalometer, T640Status
from ..default.aerosol.editing.t640 import EditingT640
from .dmps import DMPSStatus, DMPSDistribution, DMPSCounts
from .pops import POPSCounts
from .counts import RealtimeParticleConcentration, EditingParticleConcentration, ADMagicCPC250StatusStatusSecondary
from .ecotechnephelometer import NephelometerStatusSecondary, NephelometerZeroSecondary
from .optical import OpticalScatteringSecondary, EditingScatteringSecondary, EditingBackScatteringSecondary
from .green import Green


station_views = detach(aerosol_views, ozone_views)

station_views['aerosol-raw-counts'] = POPSCounts('aerosol-raw')
station_views['aerosol-editing-counts'] = EditingParticleConcentration()
station_views['aerosol-clean-counts'] = POPSCounts('aerosol-clean')
station_views['aerosol-avgh-counts'] = POPSCounts('aerosol-avgh')
station_views['aerosol-realtime-counts'] = RealtimeParticleConcentration('aerosol-realtime')

station_views['aerosol-raw-cpcstatus2'] = ADMagicCPC250StatusStatusSecondary('aerosol-raw')
station_views['aerosol-realtime-cpcstatus2'] = ADMagicCPC250StatusStatusSecondary('aerosol-realtime', realtime=True)

station_views['aerosol-raw-mass'] = T640MassAethalometer('aerosol-raw')
station_views['aerosol-raw-t640status'] = T640Status('aerosol-raw')
station_views['aerosol-editing-mass'] = EditingT640('aerosol')
station_views['aerosol-clean-mass'] = T640MassAethalometer('aerosol-clean')
station_views['aerosol-avgh-mass'] = T640MassAethalometer('aerosol-avgh')
station_views['aerosol-realtime-mass'] = T640MassAethalometer('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-t640status'] = T640Status('aerosol-realtime', realtime=True)

station_views['aerosol-raw-dmps'] = DMPSDistribution('aerosol-raw')
station_views['aerosol-raw-dmpsstatus'] = DMPSStatus('aerosol-raw')
station_views['aerosol-editing-dmps'] = DMPSDistribution('aerosol-editing')
station_views['aerosol-clean-dmps'] = DMPSDistribution('aerosol-clean')
station_views['aerosol-avgh-dmps'] = DMPSDistribution('aerosol-avgh')

station_views['aerosol-raw-pops'] = POPSDistribution('aerosol-raw')
station_views['aerosol-raw-popsstatus'] = POPSStatus('aerosol-raw')
station_views['aerosol-editing-pops'] = POPSDistribution('aerosol-editing')
station_views['aerosol-clean-pops'] = POPSDistribution('aerosol-clean')
station_views['aerosol-avgh-pops'] = POPSDistribution('aerosol-avgh')
station_views['aerosol-realtime-pops'] = POPSDistribution('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-popsstatus'] = POPSStatus('aerosol-realtime', realtime=True)

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



def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
