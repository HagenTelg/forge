import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.temperature import Temperature
from ..default.aerosol.pressure import BasicPressure
from ..default.aerosol.ecotechnephelometer import NephelometerStatus


station_views = detach(aerosol_views)

measurements = OrderedDict([
    ('{code}neph', '{code}_S11 (neph sample)'),
    ('{code}nephcell', '{code}x_S11 (neph cell)'),
])
omit_traces = {'TDnephcell', 'Unephcell'}
station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements=measurements, 
                                                       omit_traces=omit_traces)
station_views['aerosol-realtime-temperature'] = Temperature('aerosol-realtime', measurements=measurements, 
                                                            omit_traces=omit_traces, realtime=True)

station_views['aerosol-raw-pressure'] = BasicPressure('aerosol-raw')
station_views['aerosol-realtime-pressure'] = BasicPressure('aerosol-realtime', realtime=True)

station_views['aerosol-raw-nephelometerstatus'] = NephelometerStatus('aerosol-raw')
station_views['aerosol-realtime-nephelometerstatus'] = NephelometerStatus('aerosol-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
