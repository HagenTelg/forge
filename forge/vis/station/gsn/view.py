import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.tsi377Xcpc import TSI3776CPCStatus
from ..default.aerosol.temperature import Temperature


station_views = detach(aerosol_views)


station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements=OrderedDict([
    ('{code}sample', '{code}_V11 (sample)'),
    ('{code}nephinlet', '{code}u_S11 (neph inlet)'),
    ('{code}neph', '{code}_S11 (neph sample)'),
    ('{code}line2', '{code}_V21 (Aeth, COSMOS, PAX, ECOC)'),
    ('{code}line3', '{code}_V31 (APS, SMPS)'),
]), omit_traces={'TDnephinlet'})


station_views['aerosol-raw-cpcstatus'] = TSI3776CPCStatus('aerosol-raw')


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
