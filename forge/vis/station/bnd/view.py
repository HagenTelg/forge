import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.wind import Wind
from .counts import ParticleConcentration, EditingParticleConcentration, ADMagicCPC250StatusSecondary


station_views = detach(aerosol_views)

station_views['aerosol-raw-counts'] = ParticleConcentration('aerosol-raw')
station_views['aerosol-realtime-counts'] = ParticleConcentration('aerosol-realtime', realtime=True)
station_views['aerosol-editing-counts'] = EditingParticleConcentration()
station_views['aerosol-clean-counts'] = ParticleConcentration('aerosol-clean')
station_views['aerosol-avgh-counts'] = ParticleConcentration('aerosol-avgh')
station_views['aerosol-raw-cpcstatus2'] = ADMagicCPC250StatusSecondary('aerosol-raw')
station_views['aerosol-realtime-cpcstatus2'] = ADMagicCPC250StatusSecondary('aerosol-raw', realtime=True)

measurements = OrderedDict([
    ('{code}aerosol', '{type} Aerosol'),
    ('{code}grad', '{type} GRAD'),
])
station_views['aerosol-raw-wind'] = Wind('aerosol-raw', measurements=measurements)
station_views['aerosol-editing-wind'] = Wind('aerosol-editing', measurements=measurements)
station_views['aerosol-clean-wind'] = Wind('aerosol-clean', measurements=measurements)
station_views['aerosol-avgh-wind'] = Wind('aerosol-avgh', measurements=measurements)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
