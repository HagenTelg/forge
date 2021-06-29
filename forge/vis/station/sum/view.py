import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views, ozone_views, met_views
from ..default.aerosol.admagiccpc import ADMagicCPC200Status
from ..default.met.wind import Wind as MetWind
from ..default.met.editing.wind import EditingWindSpeed as MetEditingWindSpeed
from ..default.met.editing.wind import EditingWindDirection as MetEditingWindDirection
from .pressure import Pressure


station_views = detach(aerosol_views, ozone_views, met_views)


station_views['aerosol-raw-pressure'] = Pressure('aerosol-raw')
station_views['aerosol-raw-cpcstatus'] = ADMagicCPC200Status('aerosol-raw')


measurements = OrderedDict([
    ('{code}ambient', '{type} at 10m'),
    ('{code}2', '{type} at 10m (RMY)'),
])
station_views['met-raw-wind'] = MetWind('met-raw-wind', measurements=measurements)
station_views['met-clean-wind'] = MetWind('met-clean-wind', measurements=measurements)
station_views['met-avgh-wind'] = MetWind('met-avgh-wind', measurements=measurements)
measurements = OrderedDict([
    ('{code}ambient', '{mode} at 10m'),
    ('{code}2', '{mode} at 10m (RMY)'),
])
station_views['met-editing-windspeed'] = MetEditingWindSpeed(measurements=measurements)
station_views['met-editing-winddirection'] = MetEditingWindDirection(measurements=measurements)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
