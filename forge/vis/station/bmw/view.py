import typing
from ..default.view import View, detach, ozone_views, radiation_views


station_views = detach(ozone_views, radiation_views)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
