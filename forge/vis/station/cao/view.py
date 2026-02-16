import typing
from collections import OrderedDict
from ..default.view import detach, View, met_views
from ..default.met.temperature import Temperature as MetTemperature
from ..default.met.wind import Wind as MetWind
from ..default.met.editing.temperature import EditingTemperature as MetEditingTemperature, EditingRH as MetEditingRH, EditingDewpoint as MetEditingDewpoint
from ..default.met.editing.wind import EditingWindSpeed as MetEditingWindSpeed, EditingWindDirection as MetEditingWindDirection
from .tach import Tachometer, EditingTachometer


station_views = detach(met_views)

measurements = OrderedDict([
    ('{code}ambient', '{type} at 30m'),
    ('{code}2', '{type} at 100m'),
    ('{code}3', '{type} at 495m'),
])
station_views['met-raw-temperature'] = MetTemperature('met-raw-temperature', measurements=measurements)
station_views['met-clean-temperature'] = MetTemperature('met-clean-temperature', measurements=measurements)
station_views['met-avgh-temperature'] = MetTemperature('met-avgh-temperature', measurements=measurements)
measurements = OrderedDict([
    ('{code}ambient', '{type} at 30m'),
    ('{code}2', '{type} at 100m'),
    ('{code}3', '{type} at 495m'),
])
station_views['met-editing-temperature'] = MetEditingTemperature(measurements=measurements)
station_views['met-editing-rh'] = MetEditingRH(measurements=measurements)
station_views['met-editing-dewpoint'] = MetEditingDewpoint(measurements=measurements)

wind_measurements = OrderedDict([
    ('{code}ambient', '{type} at 30m'),
    ('{code}2', '{type} at 100m'),
    ('{code}3', '{type} at 495m'),
])
station_views['met-raw-wind'] = MetWind('met-raw-wind', measurements=wind_measurements)
station_views['met-clean-wind'] = MetWind('met-clean-wind', measurements=wind_measurements)
station_views['met-avgh-wind'] = MetWind('met-avgh-wind', measurements=wind_measurements)
measurements = OrderedDict([
    ('{code}ambient', '{mode} at 30m'),
    ('{code}2', '{mode} at 100m'),
    ('{code}3', '{mode} at 495m'),
])
station_views['met-editing-windspeed'] = MetEditingWindSpeed(measurements=measurements)
station_views['met-editing-winddirection'] = MetEditingWindDirection(measurements=measurements)

station_views['met-raw-tach'] = Tachometer('met-raw')
station_views['met-clean-tach'] = Tachometer('met-clean')
station_views['met-avgh-tach'] = Tachometer('met-avgh')
station_views['met-editing-tach'] = EditingTachometer()



def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
