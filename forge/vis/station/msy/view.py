import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.temperature import Temperature
from ..default.aerosol.pressure import BasicPressure
from ..default.aerosol.ecotechnephelometer import NephelometerStatus
from ..default.aerosol.maap import MAAP5012Optical, MAAP5012Status
from ..default.aerosol.editing.maap import EditingMAA5012P


station_views = detach(aerosol_views)


station_views['aerosol-raw-maap'] = MAAP5012Optical('aerosol-raw')
station_views['aerosol-editing-maap'] = EditingMAA5012P('aerosol')
station_views['aerosol-clean-maap'] = MAAP5012Optical('aerosol-clean')
station_views['aerosol-avgh-maap'] = MAAP5012Optical('aerosol-avgh')
station_views['aerosol-raw-maapstatus'] = MAAP5012Status('aerosol-raw')

station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements=OrderedDict([
    ('{code}neph', '{code}_S11 (neph sample)'),
    ('{code}nephcell', '{code}x_S11 (neph cell)'),
]), omit_traces={'TDnephcell', 'Unephcell'})

station_views['aerosol-raw-pressure'] = BasicPressure('aerosol-raw')

station_views['aerosol-raw-nephelometerstatus'] = NephelometerStatus('aerosol-raw')


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
