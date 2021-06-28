import typing
from ..default.mode import Mode, ViewList, detach, ozone_modes, met_modes


station_modes = detach(ozone_modes, met_modes)

station_modes['met-raw'].insert(ViewList.Entry('met-raw-precipitation', "Precipitation"))
station_modes['met-editing'].insert(ViewList.Entry('met-editing-precipitation', "Precipitation"))
station_modes['met-clean'].insert(ViewList.Entry('met-clean-precipitation', "Precipitation"))
station_modes['met-avgh'].insert(ViewList.Entry('met-avgh-precipitation', "Precipitation"))


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
