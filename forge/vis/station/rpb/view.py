import typing
from ..default.view import detach, View, aerosol_views, aerosol_public, ozone_views


station_views = detach(aerosol_views, aerosol_public, ozone_views)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
