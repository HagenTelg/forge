import typing
from ..default.mode import Mode, radiation_modes


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return radiation_modes.get(mode_name)
