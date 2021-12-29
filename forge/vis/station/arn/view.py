import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.temperature import Temperature
from ..default.aerosol.tsi377Xcpc import TSI3776CPCStatus
from .flow import DilutionFlow
from .pressure import Pressure


station_views = detach(aerosol_views)

station_views['aerosol-raw-flow'] = DilutionFlow('aerosol-raw')

measurements=OrderedDict([
    ('{code}nephinlet', '{code}u_S11 (neph inlet)'),
    ('{code}neph', '{code}_S11 (neph sample)'),
    ('{code}sample', '{code}_Q11 (sample)'),
    ('{code}dilution', '{code}_Q12 (dilution)'),
])
omit_traces={'TDnephinlet', 'Usample', 'TDsample', 'Udilution', 'TDdilution'}
station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements=measurements,
                                                       omit_traces=omit_traces)
station_views['aerosol-realtime-temperature'] = Temperature('aerosol-realtime', measurements=measurements,
                                                            omit_traces=omit_traces, realtime=True)

station_views['aerosol-raw-pressure'] = Pressure('aerosol-raw')
station_views['aerosol-realtime-pressure'] = Pressure('aerosol-realtime', realtime=True)

station_views['aerosol-raw-cpcstatus'] = TSI3776CPCStatus('aerosol-raw')
station_views['aerosol-realtime-cpcstatus'] = TSI3776CPCStatus('aerosol-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
