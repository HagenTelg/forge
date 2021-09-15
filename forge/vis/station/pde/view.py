import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.flow import DilutionFlow
from ..default.aerosol.temperature import Temperature
from ..default.aerosol.ecotechnephelometer import NephelometerStatus
from .optical import OpticalCLAPSecondary, EditingCLAPSecondary
from .clap import CLAPStatusSecondary
from .clouds import Clouds
from .hurricane import Hurricane


station_views = detach(aerosol_views)

station_views['aerosol-raw-opticalclap2'] = OpticalCLAPSecondary('aerosol-raw')
station_views['aerosol-editing-clap2'] = EditingCLAPSecondary()
station_views['aerosol-clean-opticalclap2'] = OpticalCLAPSecondary('aerosol-clean')
station_views['aerosol-avgh-opticalclap2'] = OpticalCLAPSecondary('aerosol-avgh')
station_views['aerosol-raw-clapstatus2'] = CLAPStatusSecondary('aerosol-raw')

station_views['aerosol-raw-flow'] = DilutionFlow('aerosol-raw')

station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements=OrderedDict([
    ('{code}inlet', '{code}_V51 (inlet)'),
    ('{code}sample', '{code}_V11 (sample)'),
    ('{code}neph', '{code}_S11 (neph sample)'),
    ('{code}nephcell', '{code}x_S11 (neph cell)'),
    ('{code}room', '{code}_V01 (room)'),
    ('{code}ambient', 'Ambient {type}'),
    ('{code}pwd', 'PWD ambient {type}'),
]), omit_traces={'TDnephcell', 'Unephcell', 'TDpwd', 'Upwd'})

station_views['aerosol-raw-clouds'] = Clouds('aerosol-raw')
station_views['aerosol-editing-clouds'] = Clouds('aerosol-editing')
station_views['aerosol-clean-clouds'] = Clouds('aerosol-clean')
station_views['aerosol-avgh-clouds'] = Clouds('aerosol-avgh')

station_views['aerosol-raw-hurricane'] = Hurricane('aerosol-raw')

station_views['aerosol-raw-nephelometerstatus'] = NephelometerStatus('aerosol-raw')


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
