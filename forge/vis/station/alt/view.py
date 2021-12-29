import typing
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.pressure import BasicPressure
from ..default.aerosol.psap import PSAPStatus
from .counts import ParticleConcentration, EditingParticleConcentration, TSI3772CPCStatus, TSI3772CPCStatusSecondary
from .clap import CLAPStatusSecondary
from .aethalometer import AE31Optical, AE31, AE31Status, AE31OpticalStatus, EditingAE31
from .aethalometer import AE33Optical, AE33, AE33Status, AE33OpticalStatus, EditingAE33
from .green import Green
from .optical import OpticalCLAPSecondary, OpticalPSAP, EditingCLAPSecondary, EditingPSAP
from .smps import SMPSDistribution, SMPSCounts
from .grimm import GrimmDistribution, GrimmStatus


station_views = detach(aerosol_views)

station_views['aerosol-raw-counts'] = SMPSCounts('aerosol-raw')
station_views['aerosol-editing-counts'] = EditingParticleConcentration()
station_views['aerosol-clean-counts'] = SMPSCounts('aerosol-clean')
station_views['aerosol-avgh-counts'] = SMPSCounts('aerosol-avgh')
station_views['aerosol-realtime-counts'] = ParticleConcentration('aerosol-realtime', realtime=True)
station_views['aerosol-raw-cpcstatus'] = TSI3772CPCStatus('aerosol-raw')
station_views['aerosol-raw-cpcstatus2'] = TSI3772CPCStatusSecondary('aerosol-raw')
station_views['aerosol-realtime-cpcstatus'] = TSI3772CPCStatus('aerosol-raw', realtime=True)
station_views['aerosol-realtime-cpcstatus2'] = TSI3772CPCStatusSecondary('aerosol-raw', realtime=True)

station_views['aerosol-raw-pressure'] = BasicPressure('aerosol-raw')
station_views['aerosol-realtime-pressure'] = BasicPressure('aerosol-realtime', realtime=True)

station_views['aerosol-raw-opticalclap2'] = OpticalCLAPSecondary('aerosol-raw')
station_views['aerosol-editing-clap2'] = EditingCLAPSecondary()
station_views['aerosol-clean-opticalclap2'] = OpticalCLAPSecondary('aerosol-clean')
station_views['aerosol-avgh-opticalclap2'] = OpticalCLAPSecondary('aerosol-avgh')
station_views['aerosol-realtime-opticalclap2'] = OpticalCLAPSecondary('aerosol-realtime', realtime=True)
station_views['aerosol-raw-clapstatus2'] = CLAPStatusSecondary('aerosol-raw')
station_views['aerosol-realtime-clapstatus2'] = CLAPStatusSecondary('aerosol-realtime', realtime=True)

station_views['aerosol-raw-opticalpsap'] = OpticalPSAP('aerosol-raw')
station_views['aerosol-editing-psap'] = EditingPSAP()
station_views['aerosol-clean-opticalpsap'] = OpticalPSAP('aerosol-clean')
station_views['aerosol-avgh-opticalpsap'] = OpticalPSAP('aerosol-avgh')
station_views['aerosol-realtime-opticalpsap'] = OpticalPSAP('aerosol-realtime', realtime=True)
station_views['aerosol-raw-psapstatus'] = PSAPStatus('aerosol-raw')
station_views['aerosol-realtime-psapstatus'] = PSAPStatus('aerosol-realtime', realtime=True)

station_views['aerosol-raw-ae31'] = AE31('aerosol-raw')
station_views['aerosol-raw-ae31status'] = AE31Status('aerosol-raw')
station_views['aerosol-realtime-ae31'] = AE31('aerosol-raw', realtime=True)
station_views['aerosol-realtime-ae31status'] = AE31Status('aerosol-raw', realtime=True)
station_views['aerosol-editing-ae31'] = EditingAE31()
station_views['aerosol-editing-ae31status'] = AE31OpticalStatus('aerosol-editing')
station_views['aerosol-clean-ae31'] = AE31Optical('aerosol-clean')
station_views['aerosol-avgh-ae31'] = AE31Optical('aerosol-avgh')

station_views['aerosol-raw-ae33'] = AE33('aerosol-raw')
station_views['aerosol-raw-ae33status'] = AE33Status('aerosol-raw')
station_views['aerosol-realtime-ae33'] = AE33('aerosol-raw', realtime=True)
station_views['aerosol-realtime-ae33status'] = AE33Status('aerosol-raw', realtime=True)
station_views['aerosol-editing-ae33'] = EditingAE33()
station_views['aerosol-editing-ae33status'] = AE33OpticalStatus('aerosol-editing')
station_views['aerosol-clean-ae33'] = AE33Optical('aerosol-clean')
station_views['aerosol-avgh-ae33'] = AE33Optical('aerosol-avgh')

station_views['aerosol-raw-green'] = Green('aerosol-raw')
station_views['aerosol-clean-green'] = Green('aerosol-clean')
station_views['aerosol-avgh-green'] = Green('aerosol-avgh')
station_views['aerosol-realtime-green'] = Green('aerosol-realtime', realtime=True)

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
