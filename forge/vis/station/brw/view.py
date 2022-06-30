import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views, ozone_views, met_views
from ..default.aerosol.wind import Wind
from ..default.aerosol.temperature import Temperature
from ..default.aerosol.ccn import CCNStatus
from ..default.met.temperature import Temperature as MetTemperature
from ..default.met.wind import Wind as MetWind
from ..default.met.editing.temperature import EditingTemperature as MetEditingTemperature
from ..default.met.editing.wind import EditingWindSpeed as MetEditingWindSpeed
from ..default.met.editing.wind import EditingWindDirection as MetEditingWindDirection
from ..default.met.tower import TowerTemperatureDifference as MetTowerTemperatureDifference
from ..default.met.editing.tower import EditingTowerTemperatureDifference as MetEditingTowerTemperatureDifference
from .flow import Flow
from .filter import FilterStatus, SecondFilterStatus
from .umac import UMACStatus
from .counts import ParticleConcentration, EditingParticleConcentration


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

station_views['aerosol-raw-counts'] = ParticleConcentration('aerosol-raw')
station_views['aerosol-realtime-counts'] = ParticleConcentration('aerosol-realtime', realtime=True)
station_views['aerosol-editing-counts'] = EditingParticleConcentration()
station_views['aerosol-clean-counts'] = ParticleConcentration('aerosol-clean')
station_views['aerosol-avgh-counts'] = ParticleConcentration('aerosol-avgh')
station_views['aerosol-raw-filterstatus'] = FilterStatus('aerosol-raw')
station_views['aerosol-raw-filterstatus2'] = SecondFilterStatus('aerosol-raw')
station_views['aerosol-raw-umacstatus'] = UMACStatus('aerosol-raw')
station_views['aerosol-realtime-filterstatus'] = FilterStatus('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-filterstatus2'] = SecondFilterStatus('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-umacstatus'] = UMACStatus('aerosol-realtime', realtime=True)
station_views['aerosol-raw-ccnstatus'] = CCNStatus('aerosol-raw')
station_views['aerosol-realtime-ccnstatus'] = CCNStatus('aerosol-raw', realtime=True)


measurements = OrderedDict([
    ('{code}ambient', '{type} at 2m'),
    ('{code}2', '{type} at 10m'),
    ('{code}3', '{type} at 30m'),
])
omit_traces = {'U2', 'TD2', 'U3', 'TD3'}
station_views['met-raw-temperature'] = MetTemperature('met-raw-temperature', measurements=measurements,
                                                      omit_traces=omit_traces)
station_views['met-clean-temperature'] = MetTemperature('met-clean-temperature', measurements=measurements,
                                                        omit_traces=omit_traces)
station_views['met-avgh-temperature'] = MetTemperature('met-avgh-temperature', measurements=measurements,
                                                       omit_traces=omit_traces)
station_views['met-editing-temperature'] = MetEditingTemperature(measurements=OrderedDict([
    ('{code}ambient', '{mode} at 2m'),
    ('{code}2', '{mode} at 10m'),
    ('{code}3', '{mode} at 30m'),
]))

measurements = OrderedDict([
    ('{code}ambient', '{type} at 10m'),
    ('{code}2', '{type} at 2m'),
    ('{code}3', '{type} at 30m'),
])
station_views['met-raw-wind'] = MetWind('met-raw-wind', measurements=measurements)
station_views['met-clean-wind'] = MetWind('met-clean-wind', measurements=measurements)
station_views['met-avgh-wind'] = MetWind('met-avgh-wind', measurements=measurements)
measurements = OrderedDict([
    ('{code}ambient', '{mode} at 10m'),
    ('{code}2', '{mode} at 2m'),
    ('{code}3', '{mode} at 30m'),
])
station_views['met-editing-windspeed'] = MetEditingWindSpeed(measurements=measurements)
station_views['met-editing-winddirection'] = MetEditingWindDirection(measurements=measurements)

station_views['met-raw-tower'] = MetTowerTemperatureDifference('met-raw')
station_views['met-editing-tower'] = MetEditingTowerTemperatureDifference()



def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
