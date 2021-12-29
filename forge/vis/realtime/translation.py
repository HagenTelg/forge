import typing
from . import Translator
from forge.vis.station.lookup import station_data


def get_translator(station: str) -> typing.Optional[Translator]:
    return station_data(station, 'realtime', 'translator')(station)
