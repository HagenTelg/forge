import typing
from ..default.view import View, ozone_views


def get(station: str, view_name: str) -> typing.Optional[View]:
    return ozone_views.get(view_name)
