import typing
from collections import OrderedDict
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.wind import Wind


station_views = detach(aerosol_views)

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
