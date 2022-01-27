import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.ecotechnephelometer import NephelometerStatus
from ..default.aerosol.temperature import Temperature
from ..default.aerosol.pressure import BasicPressure
from ..default.aerosol.flow import BasicFlow
from .counts import ParticleConcentration, EditingParticleConcentration, ADMagicCPC200Status, ADMagicCPC200StatusStatusSecondary


station_views = detach(aerosol_views)


station_views['aerosol-raw-counts'] = ParticleConcentration('aerosol-raw')
station_views['aerosol-editing-counts'] = EditingParticleConcentration()
station_views['aerosol-clean-counts'] = ParticleConcentration('aerosol-clean')
station_views['aerosol-avgh-counts'] = ParticleConcentration('aerosol-avgh')
station_views['aerosol-raw-cpcstatus'] = ADMagicCPC200Status('aerosol-raw')
station_views['aerosol-raw-cpcstatus2'] = ADMagicCPC200StatusStatusSecondary('aerosol-raw')


station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements=OrderedDict([
    ('{code}sample', '{code}_V11 (sample)'),
    ('{code}neph', '{code}_S11 (neph sample)'),
    ('{code}nephcell', '{code}x_S11 (neph cell)'),
    ('{code}rack', '{code}_V21 (rack)'),
    ('{code}ambient', 'Ambient {type}'),
]), omit_traces={'TDnephcell', 'Unephcell'})


station_views['aerosol-raw-pressure'] = BasicPressure('aerosol-raw')
station_views['aerosol-raw-flow'] = BasicFlow('aerosol-raw')
station_views['aerosol-raw-nephelometerstatus'] = NephelometerStatus('aerosol-raw')


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
