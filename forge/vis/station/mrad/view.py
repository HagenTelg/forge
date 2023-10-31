import typing
from forge.vis.view.solar import SolarPosition
from ..default.view import View
from . import Site
from .radiation import EditingShortwave, EditingLongwave, EditingPyranometerTemperature, EditingAlbedo, EditingTotalRatio
from .ambient import Ambient


# Nantucket (inexact)
LATITUDE = 41.282778
LONGITUDE = -70.099444

sites: typing.List[Site] = [
    Site('RMSURFRAD', 'nan', "Nantucket"),
    Site('RADSYS2', 'bid', "Block Island", include_spn1=True),
]

views: typing.Dict[str, View] = {
    'radiation-editing-shortwave': EditingShortwave(LATITUDE, LONGITUDE, 'radiation', sites),
    'radiation-editing-longwave': EditingLongwave(LATITUDE, LONGITUDE, 'radiation', sites),
    'radiation-editing-pyranometertemperature': EditingPyranometerTemperature(LATITUDE, LONGITUDE, 'radiation', sites),
    'radiation-editing-albedo': EditingAlbedo(LATITUDE, LONGITUDE, 'radiation', sites),
    'radiation-editing-totalratio': EditingTotalRatio(LATITUDE, LONGITUDE, 'radiation', sites),

    'radiation-editing-ambient': Ambient('radiation-editing-ambient', sites),
    'radiation-editing-solarposition': SolarPosition(LATITUDE, LONGITUDE),
}


def get(station: str, view_name: str) -> typing.Optional[View]:
    return views.get(view_name)
