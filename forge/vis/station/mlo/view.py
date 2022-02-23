import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views, ozone_views, met_views
from ..default.met.wind import Wind as MetWind
from ..default.met.temperature import Temperature as MetTemperature
from ..default.met.precipitation import Precipitation as MetPrecipitation
from ..default.met.tower import TowerTemperatureDifference as MetTowerTemperatureDifference
from ..default.met.editing.wind import EditingWindSpeed as MetEditingWindSpeed
from ..default.met.editing.wind import EditingWindDirection as MetEditingWindDirection
from ..default.met.editing.temperature import EditingTemperature as MetEditingTemperature
from ..default.met.editing.precipitation import EditingPrecipitation as MetEditingPrecipitation
from ..default.met.editing.tower import EditingTowerTemperatureDifference as MetEditingTowerTemperatureDifference
from .contamination import EditingContaminationDetails


station_views = detach(aerosol_views, ozone_views, met_views)

station_views['aerosol-editing-contaminationdetails'] = EditingContaminationDetails()

measurements = OrderedDict([
    ('{code}ambient', '{type} at 2m'),
    ('{code}2', '{type} at 10m'),
    ('{code}3', '{type} at 38m'),
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
    ('{code}3', '{mode} at 38m'),
]))

measurements = OrderedDict([
    ('{code}ambient', '{type} at 10m'),
    ('{code}2', '{type} at 38m'),
])
station_views['met-raw-wind'] = MetWind('met-raw-wind', measurements=measurements)
station_views['met-clean-wind'] = MetWind('met-clean-wind', measurements=measurements)
station_views['met-avgh-wind'] = MetWind('met-avgh-wind', measurements=measurements)
measurements = OrderedDict([
    ('{code}ambient', '{mode} at 10m'),
    ('{code}2', '{mode} at 38m'),
])
station_views['met-editing-windspeed'] = MetEditingWindSpeed(measurements=measurements)
station_views['met-editing-winddirection'] = MetEditingWindDirection(measurements=measurements)

station_views['met-raw-precipitation'] = MetPrecipitation('met-raw')
station_views['met-clean-precipitation'] = MetPrecipitation('met-clean')
station_views['met-editing-precipitation'] = MetEditingPrecipitation()
station_views['met-avgh-precipitation'] = MetPrecipitation('met-avgh')

station_views['met-raw-tower'] = MetTowerTemperatureDifference('met-raw')
station_views['met-editing-tower'] = MetEditingTowerTemperatureDifference()


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
