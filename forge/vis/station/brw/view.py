import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views, ozone_views, met_views
from ..default.aerosol.wind import Wind
from ..default.aerosol.temperature import Temperature
from ..default.met.temperature import Temperature as MetTemperature
from ..default.met.editing.temperature import EditingTemperature as MetEditingTemperature
from .flow import Flow
from .filter import FilterStatus
from .umac import UMACStatus


station_views = detach(aerosol_views, ozone_views, met_views)


station_views['aerosol-raw-wind'] = Wind('aerosol-raw', measurements=OrderedDict([
    ('{code}ambient', '{type} ambient'),
    ('{code}filter', '{type} filter'),
]))

station_views['aerosol-raw-flow'] = Flow('aerosol-raw')
station_views['aerosol-realtime-flow'] = Flow('aerosol-realtime', realtime=True)

station_views['aerosol-raw-temperature'] = Temperature('aerosol-raw', measurements=OrderedDict([
    ('{code}inlet', '{code}_V51 (inlet)'),
    ('{code}sample', '{code}_V11 (sample)'),
    ('{code}nephinlet', '{code}u_S11 (neph inlet)'),
    ('{code}neph', '{code}_S11 (neph sample)'),
    ('{code}filter', 'Filter {type}'),
    ('{code}filterrack', 'Filter rack {type}'),
    ('{code}ambient', 'Ambient {type}'),
]), omit_traces={'TDnephinlet', 'TDfilterrack', 'Ufilterrack'})
station_views['aerosol-realtime-temperature'] = Temperature('aerosol-realtime', measurements=OrderedDict([
    ('{code}inlet', '{code}_V51 (inlet)'),
    ('{code}sample', '{code}_V11 (sample)'),
    ('{code}nephinlet', '{code}u_S11 (neph inlet)'),
    ('{code}neph', '{code}_S11 (neph sample)'),
    ('{code}filter', 'Filter {type}'),
    ('{code}filterrack', 'Filter rack {type}'),
]), omit_traces={'TDnephinlet', 'TDfilterrack', 'Ufilterrack'}, realtime=True)

station_views['aerosol-raw-filterstatus'] = FilterStatus('aerosol-raw')
station_views['aerosol-raw-umacstatus'] = UMACStatus('aerosol-raw')
station_views['aerosol-realtime-filterstatus'] = FilterStatus('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-umacstatus'] = UMACStatus('aerosol-realtime', realtime=True)


measurements = OrderedDict([
    ('{code}ambient', '{type} at 2m'),
    ('{code}2', '{type} at 16m'),
])
omit_traces = {'U2', 'TD2'}
station_views['met-raw-temperature'] = MetTemperature('met-raw-temperature', measurements=measurements,
                                                      omit_traces=omit_traces)
station_views['met-clean-temperature'] = MetTemperature('met-clean-temperature', measurements=measurements,
                                                        omit_traces=omit_traces)
station_views['met-avgh-temperature'] = MetTemperature('met-avgh-temperature', measurements=measurements,
                                                       omit_traces=omit_traces)
station_views['met-editing-temperature'] = MetEditingTemperature(measurements=OrderedDict([
    ('{code}ambient', '{mode} at 2m'),
    ('{code}2', '{mode} at 16m'),
]))



def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
