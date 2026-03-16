import typing
from ..default.view import View, radiation_views


def get(station: str, view_name: str) -> typing.Optional[View]:
    return radiation_views.get(view_name)
