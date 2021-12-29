import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.aethalometer import AE31, AE31Status, AE31OpticalStatus
from ..default.aerosol.flow import DilutionFlow
from ..default.aerosol.temperature import Temperature
from .counts import ParticleConcentration, EditingParticleConcentration, TSI3772CPCStatusSecondary
from .clouds import Clouds
from .hurricane import Hurricane


station_views = detach(aerosol_views)

station_views['aerosol-raw-counts'] = ParticleConcentration('aerosol-raw')
station_views['aerosol-realtime-counts'] = ParticleConcentration('aerosol-realtime', realtime=True)
station_views['aerosol-editing-counts'] = EditingParticleConcentration()
station_views['aerosol-clean-counts'] = ParticleConcentration('aerosol-clean')
station_views['aerosol-avgh-counts'] = ParticleConcentration('aerosol-avgh')

station_views['aerosol-raw-cpcstatus2'] = TSI3772CPCStatusSecondary('aerosol-raw')
station_views['aerosol-realtime-cpcstatus2'] = TSI3772CPCStatusSecondary('aerosol-realtime', realtime=True)

station_views['aerosol-raw-aethalometer'] = AE31('aerosol-raw')
station_views['aerosol-raw-aethalometerstatus'] = AE31Status('aerosol-raw')
station_views['aerosol-realtime-aethalometer'] = AE31('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-aethalometerstatus'] = AE31Status('aerosol-realtime', realtime=True)
station_views['aerosol-editing-aethalometerstatus'] = AE31OpticalStatus('aerosol-editing')

station_views['aerosol-raw-flow'] = DilutionFlow('aerosol-raw')
station_views['aerosol-realtime-flow'] = DilutionFlow('aerosol-realtime', realtime=True)

measurements = OrderedDict([
    ('{code}inlet', '{code}_V51 (inlet)'),
    ('{code}sample', '{code}_V11 (sample)'),
    ('{code}nephinlet', '{code}u_S11 (neph inlet)'),
    ('{code}neph', '{code}_S11 (neph sample)'),
    ('{code}room', '{code}_V01 (room)'),
    ('{code}ambient', 'Ambient {type}'),
    ('{code}pwd', 'PWD ambient {type}'),
])
omit_traces = {'TDnephinlet', 'TDpwd', 'Upwd'}
station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements=measurements,
                                                       omit_traces=omit_traces)
station_views['aerosol-realtime-temperature'] = Temperature('aerosol-realtime', measurements=measurements,
                                                            omit_traces=omit_traces, realtime=True)

station_views['aerosol-raw-clouds'] = Clouds('aerosol-raw')
station_views['aerosol-realtime-clouds'] = Clouds('aerosol-realtime', realtime=True)
station_views['aerosol-editing-clouds'] = Clouds('aerosol-editing')
station_views['aerosol-clean-clouds'] = Clouds('aerosol-clean')
station_views['aerosol-avgh-clouds'] = Clouds('aerosol-avgh')

station_views['aerosol-raw-hurricane'] = Hurricane('aerosol-raw')
station_views['aerosol-realtime-hurricane'] = Hurricane('aerosol-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
