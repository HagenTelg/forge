import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views, aerosol_public, ozone_views, ozone_public, met_views, radiation_views
from ..default.met.wind import Wind as MetWind
from ..default.met.temperature import Temperature as MetTemperature
from ..default.met.precipitation import Precipitation as MetPrecipitation
from ..default.met.tower import TowerTemperatureDifference as MetTowerTemperatureDifference
from ..default.met.editing.wind import EditingWindSpeed as MetEditingWindSpeed
from ..default.met.editing.wind import EditingWindDirection as MetEditingWindDirection
from ..default.met.editing.temperature import EditingTemperature as MetEditingTemperature
from ..default.met.editing.precipitation import EditingPrecipitation as MetEditingPrecipitation
from ..default.met.editing.tower import EditingTowerTemperatureDifference as MetEditingTowerTemperatureDifference
from ..default.radiation.ambient import Ambient as RadiationAmbient
from .counts import ParticleConcentration, EditingParticleConcentration,  ADMagicCPC250StatusSecondary
from .contamination import EditingContaminationDetails
from .pressure import Pressure


station_views = detach(aerosol_views, aerosol_public, ozone_views, ozone_public, met_views, radiation_views)

station_views['aerosol-raw-counts'] = ParticleConcentration('aerosol-raw')
station_views['aerosol-realtime-counts'] = ParticleConcentration('aerosol-realtime', realtime=True)
station_views['aerosol-editing-counts'] = EditingParticleConcentration()
station_views['aerosol-clean-counts'] = ParticleConcentration('aerosol-clean')
station_views['aerosol-avgh-counts'] = ParticleConcentration('aerosol-avgh')
station_views['aerosol-raw-cpcstatus2'] = ADMagicCPC250StatusSecondary('aerosol-raw')
station_views['aerosol-realtime-cpcstatus2'] = ADMagicCPC250StatusSecondary('aerosol-realtime', realtime=True)
station_views['aerosol-editing-contaminationdetails'] = EditingContaminationDetails()

station_views['aerosol-raw-pressure'] = Pressure('aerosol-raw')
station_views['aerosol-realtime-pressure'] = Pressure('aerosol-realtime', realtime=True)

temperature_measurements = OrderedDict([
    ('{code}ambient', '{type} at 2m'),
    ('{code}2', '{type} at 10m'),
    ('{code}3', '{type} at 38m'),
])
temperature_omit = {'U2', 'TD2', 'U3', 'TD3'}
station_views['met-raw-temperature'] = MetTemperature('met-raw-temperature', measurements=temperature_measurements,
                                                      omit_traces=temperature_omit)
station_views['met-clean-temperature'] = MetTemperature('met-clean-temperature', measurements=temperature_measurements,
                                                        omit_traces=temperature_omit)
station_views['met-avgh-temperature'] = MetTemperature('met-avgh-temperature', measurements=temperature_measurements,
                                                       omit_traces=temperature_omit)
station_views['met-editing-temperature'] = MetEditingTemperature(measurements=OrderedDict([
    ('{code}ambient', '{mode} at 2m'),
    ('{code}2', '{mode} at 10m'),
    ('{code}3', '{mode} at 38m'),
]))

wind_measurements = OrderedDict([
    ('{code}ambient', '{type} at 10m'),
    ('{code}2', '{type} at 38m'),
])
station_views['met-raw-wind'] = MetWind('met-raw-wind', measurements=wind_measurements)
station_views['met-clean-wind'] = MetWind('met-clean-wind', measurements=wind_measurements)
station_views['met-avgh-wind'] = MetWind('met-avgh-wind', measurements=wind_measurements)
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


station_views['radiation-raw-ambient'] = RadiationAmbient('radiation-raw', trh=temperature_measurements,
                                                          winds=wind_measurements,
                                                          omit_traces=temperature_omit)
station_views['radiation-editing-ambient'] = RadiationAmbient('radiation-editing', trh=temperature_measurements,
                                                              winds=wind_measurements,
                                                              omit_traces=temperature_omit)
station_views['radiation-clean-ambient'] = RadiationAmbient('radiation-clean', trh=temperature_measurements,
                                                            winds=wind_measurements,
                                                            omit_traces=temperature_omit)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
