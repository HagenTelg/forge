import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.temperature import Temperature
from ..default.aerosol.pressure import BasicPressure
from .flow import DilutionFlow


station_views = detach(aerosol_views)


station_views['aerosol-raw-flow'] = DilutionFlow('aerosol-raw')

station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements=OrderedDict([
    ('{code}nephinlet', '{code}u_S11 (neph inlet)'),
    ('{code}neph', '{code}_S11 (neph sample)'),
]), omit_traces={'TDnephinlet'})

station_views['aerosol-raw-pressure'] = BasicPressure('aerosol-raw')


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
