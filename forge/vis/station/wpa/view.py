import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.temperature import Temperature
from ..default.aerosol.ecotechnephelometer import NephelometerStatus
from ..default.aerosol.admagiccpc import ADMagicCPC200Status


station_views = detach(aerosol_views)


station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements=OrderedDict([
    ('{code}neph', '{code}_S11 (neph sample)'),
    ('{code}nephcell', '{code}x_S11 (neph cell)'),
]), omit_traces={'TDnephcell', 'Unephcell'})


station_views['aerosol-raw-nephelometerstatus'] = NephelometerStatus('aerosol-raw')

station_views['aerosol-raw-cpcstatus'] = ADMagicCPC200Status('aerosol-raw')


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
