import typing
from forge.vis.view.solar import SolarPosition
from ..default.view import View
from . import Site
from .radiation import EditingSolar, EditingIR, EditingPyranometerTemperature, EditingAlbedo, EditingTotalRatio
from .ambient import Ambient


# Brush Creek
LATITUDE = 38.859127744
LONGITUDE = -106.920904658

sites: typing.List[Site] = [
    Site('RMSURFRAD', 'cbc', "Brush Creek"),
    Site('RADSYS2', 'ckp', "Kettle Ponds", include_spn1=True),
]

views: typing.Dict[str, View] = {
    'radiation-editing-solar': EditingSolar(LATITUDE, LONGITUDE, 'radiation', sites),
    'radiation-editing-ir': EditingIR(LATITUDE, LONGITUDE, 'radiation', sites),
    'radiation-editing-pyranometertemperature': EditingPyranometerTemperature(LATITUDE, LONGITUDE, 'radiation', sites),
    'radiation-editing-albedo': EditingAlbedo(LATITUDE, LONGITUDE, 'radiation', sites),
    'radiation-editing-totalratio': EditingTotalRatio(LATITUDE, LONGITUDE, 'radiation', sites),

    'radiation-editing-ambient': Ambient('radiation-editing-ambient', sites),
    'radiation-editing-solarposition': SolarPosition(LATITUDE, LONGITUDE),
}


def get(station: str, view_name: str) -> typing.Optional[View]:
    return views.get(view_name)
