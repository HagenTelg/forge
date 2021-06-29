import typing
from ..default.mode import Mode, ozone_modes


def get(station: str, mode_name: str) -> typing.Optional[Mode]:
    return ozone_modes.get(mode_name)
