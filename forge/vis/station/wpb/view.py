import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.ecotechnephelometer import NephelometerStatus
from ..default.aerosol.temperature import Temperature
from ..default.aerosol.pressure import BasicPressure
from ..default.aerosol.flow import BasicFlow
from .counts import ParticleConcentration, EditingParticleConcentration, ADMagicCPC200Status, ADMagicCPC200StatusStatusSecondary
from .clap import CLAPStatusSecondary
from .optical import OpticalCLAPSecondary, EditingCLAPSecondary
from .green import Green


station_views = detach(aerosol_views)


station_views['aerosol-raw-counts'] = ParticleConcentration('aerosol-raw')
station_views['aerosol-realtime-counts'] = ParticleConcentration('aerosol-realtime', realtime=True)
station_views['aerosol-editing-counts'] = EditingParticleConcentration()
station_views['aerosol-clean-counts'] = ParticleConcentration('aerosol-clean')
station_views['aerosol-avgh-counts'] = ParticleConcentration('aerosol-avgh')
station_views['aerosol-raw-cpcstatus'] = ADMagicCPC200Status('aerosol-raw')
station_views['aerosol-raw-cpcstatus2'] = ADMagicCPC200StatusStatusSecondary('aerosol-raw')
station_views['aerosol-realtime-cpcstatus'] = ADMagicCPC200Status('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-cpcstatus2'] = ADMagicCPC200StatusStatusSecondary('aerosol-realtime', realtime=True)


station_views['aerosol-raw-opticalclap2'] = OpticalCLAPSecondary('aerosol-raw')
station_views['aerosol-editing-clap2'] = EditingCLAPSecondary()
station_views['aerosol-clean-opticalclap2'] = OpticalCLAPSecondary('aerosol-clean')
station_views['aerosol-avgh-opticalclap2'] = OpticalCLAPSecondary('aerosol-avgh')
station_views['aerosol-realtime-opticalclap2'] = OpticalCLAPSecondary('aerosol-realtime', realtime=True)
station_views['aerosol-raw-clapstatus2'] = CLAPStatusSecondary('aerosol-raw')
station_views['aerosol-realtime-clapstatus2'] = CLAPStatusSecondary('aerosol-realtime', realtime=True)


station_views['aerosol-raw-green'] = Green('aerosol-raw')
station_views['aerosol-clean-green'] = Green('aerosol-clean')
station_views['aerosol-avgh-green'] = Green('aerosol-avgh')
station_views['aerosol-realtime-green'] = Green('aerosol-realtime', realtime=True)


measurements = OrderedDict([
    ('{code}sample', '{code}_V11 (sample)'),
    ('{code}neph', '{code}_S11 (neph sample)'),
    ('{code}nephcell', '{code}x_S11 (neph cell)'),
    ('{code}rack', '{code}_V21 (rack)'),
    ('{code}ambient', 'Ambient {type}'),
])
omit_traces = {'TDnephcell', 'Unephcell'}
station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements=measurements, 
                                                       omit_traces=omit_traces)
station_views['aerosol-realtime-temperature'] = Temperature('aerosol-realtime', measurements=measurements, 
                                                            omit_traces=omit_traces, realtime=True)


station_views['aerosol-raw-pressure'] = BasicPressure('aerosol-raw')
station_views['aerosol-raw-flow'] = BasicFlow('aerosol-raw')
station_views['aerosol-raw-nephelometerstatus'] = NephelometerStatus('aerosol-raw')
station_views['aerosol-realtime-pressure'] = BasicPressure('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-flow'] = BasicFlow('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-nephelometerstatus'] = NephelometerStatus('aerosol-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
