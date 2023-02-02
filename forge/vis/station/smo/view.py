import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views, ozone_views, met_views, radiation_views
from ..default.met.temperature import Temperature as MetTemperature
from ..default.met.precipitation import Precipitation as MetPrecipitation
from ..default.met.editing.precipitation import EditingPrecipitation as MetEditingPrecipitation
from ..default.met.editing.temperature import EditingTemperature as MetEditingTemperature


station_views = detach(aerosol_views, ozone_views, met_views, radiation_views)

measurements = OrderedDict([
    ('{code}ambient', '{type} at 2m'),
    ('{code}2', '{type} at 17m'),
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
    ('{code}2', '{mode} at 17m'),
]))

station_views['met-raw-precipitation'] = MetPrecipitation('met-raw')
station_views['met-clean-precipitation'] = MetPrecipitation('met-clean')
station_views['met-editing-precipitation'] = MetEditingPrecipitation()
station_views['met-avgh-precipitation'] = MetPrecipitation('met-avgh')


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
