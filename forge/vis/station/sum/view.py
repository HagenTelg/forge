import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views, ozone_views, met_views, radiation_views
from ..default.aerosol.admagiccpc import ADMagicCPC200Status
from ..default.met.wind import Wind as MetWind
from ..default.met.editing.wind import EditingWindSpeed as MetEditingWindSpeed
from ..default.met.editing.wind import EditingWindDirection as MetEditingWindDirection
from ..default.radiation.ambient import Ambient as RadiationAmbient
from .pressure import Pressure


station_views = detach(aerosol_views, ozone_views, met_views, radiation_views)


station_views['aerosol-raw-pressure'] = Pressure('aerosol-raw')
station_views['aerosol-raw-cpcstatus'] = ADMagicCPC200Status('aerosol-raw')
station_views['aerosol-realtime-pressure'] = Pressure('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-cpcstatus'] = ADMagicCPC200Status('aerosol-realtime', realtime=True)


wind_measurements = OrderedDict([
    ('{code}ambient', '{type} at 10m'),
    ('{code}2', '{type} at 10m (RMY)'),
])
station_views['met-raw-wind'] = MetWind('met-raw-wind', measurements=wind_measurements)
station_views['met-clean-wind'] = MetWind('met-clean-wind', measurements=wind_measurements)
station_views['met-avgh-wind'] = MetWind('met-avgh-wind', measurements=wind_measurements)
measurements = OrderedDict([
    ('{code}ambient', '{mode} at 10m'),
    ('{code}2', '{mode} at 10m (RMY)'),
])
station_views['met-editing-windspeed'] = MetEditingWindSpeed(measurements=measurements)
station_views['met-editing-winddirection'] = MetEditingWindDirection(measurements=measurements)


station_views['radiation-raw-ambient'] = RadiationAmbient('radiation-raw', winds=wind_measurements)
station_views['radiation-editing-ambient'] = RadiationAmbient('radiation-editing', winds=wind_measurements)
station_views['radiation-clean-ambient'] = RadiationAmbient('radiation-clean', winds=wind_measurements)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
