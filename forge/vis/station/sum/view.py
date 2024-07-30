import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views, ozone_views, met_views, radiation_views
from ..default.aerosol.admagiccpc import ADMagicCPC200Status
from ..default.met.wind import Wind as MetWind
from ..default.met.editing.wind import EditingWindSpeed as MetEditingWindSpeed
from ..default.met.editing.wind import EditingWindDirection as MetEditingWindDirection
from ..default.radiation.ambient import Ambient as RadiationAmbient
from .pressure import Pressure
from .ecotechnephelometer import NephelometerStatusSecondary, NephelometerZeroSecondary
from .optical import OpticalScatteringSecondary, EditingScatteringSecondary, EditingBackScatteringSecondary


station_views = detach(aerosol_views, ozone_views, met_views, radiation_views)


station_views['aerosol-raw-pressure'] = Pressure('aerosol-raw')
station_views['aerosol-raw-cpcstatus'] = ADMagicCPC200Status('aerosol-raw')
station_views['aerosol-realtime-pressure'] = Pressure('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-cpcstatus'] = ADMagicCPC200Status('aerosol-realtime', realtime=True)


station_views['aerosol-raw-opticalscattering2'] = OpticalScatteringSecondary('aerosol-raw')
station_views['aerosol-realtime-opticalscattering2'] = OpticalScatteringSecondary('aerosol-realtime', realtime=True)
station_views['aerosol-editing-scattering2'] = EditingScatteringSecondary()
station_views['aerosol-editing-backscattering2'] = EditingBackScatteringSecondary()
station_views['aerosol-clean-opticalscattering2'] = OpticalScatteringSecondary('aerosol-clean')
station_views['aerosol-avgh-opticalscattering2'] = OpticalScatteringSecondary('aerosol-avgh')
station_views['aerosol-raw-nephelometerzero2'] = NephelometerZeroSecondary('aerosol-raw')
station_views['aerosol-raw-nephelometerstatus2'] = NephelometerStatusSecondary('aerosol-raw')
station_views['aerosol-raw-nephelometerzero2'] = NephelometerZeroSecondary('aerosol-raw')
station_views['aerosol-raw-nephelometerstatus2'] = NephelometerStatusSecondary('aerosol-raw')
station_views['aerosol-realtime-nephelometerzero2'] = NephelometerZeroSecondary('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-nephelometerstatus2'] = NephelometerStatusSecondary('aerosol-realtime', realtime=True)


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
