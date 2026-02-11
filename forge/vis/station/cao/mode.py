import typing
from ..default.mode import Mode, ViewList, detach, met_modes


station_modes = detach(met_modes)

station_modes['met-raw'].insert(ViewList.Entry('met-raw-tach', "Fan Tachometers"),
                                'met-raw-pressure')
station_modes['met-editing'].insert(ViewList.Entry('met-editing-tach', "Fan Tachometers"),
                                    'met-editing-pressure')
station_modes['met-clean'].insert(ViewList.Entry('met-clean-tach', "Fan Tachometers"),
                                  'met-clean-pressure')
station_modes['met-avgh'].insert(ViewList.Entry('met-avgh-tach', "Fan Tachometers"),
                                 'met-avgh-pressure')


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return station_modes.get(mode_name)
