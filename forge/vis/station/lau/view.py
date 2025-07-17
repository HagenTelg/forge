import typing
from ..default.view import detach, View, ozone_views, ozone_public


station_views = detach(ozone_views, ozone_public)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
