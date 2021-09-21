import typing
from ..default.view import detach, View, aerosol_views
from .counts import BMI1720CPCStatus


station_views = detach(aerosol_views)

station_views['aerosol-raw-cpcstatus'] = BMI1720CPCStatus('aerosol-raw')


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
