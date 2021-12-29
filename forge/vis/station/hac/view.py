import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.temperature import Temperature
from ..default.aerosol.pressure import BasicPressure


station_views = detach(aerosol_views)

measurements = OrderedDict([
    ('{code}nephinlet', '{code}u_S11 (neph inlet)'),
    ('{code}neph', '{code}_S11 (neph sample)'),
])
omit_traces = {'TDnephinlet'}
station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements=measurements, 
                                                       omit_traces=omit_traces)
station_views['aerosol-realtime-temperature'] = Temperature('aerosol-realtime', measurements=measurements,
                                                            omit_traces=omit_traces, realtime=True)

station_views['aerosol-raw-pressure'] = BasicPressure('aerosol-raw')
station_views['aerosol-realtime-pressure'] = BasicPressure('aerosol-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
