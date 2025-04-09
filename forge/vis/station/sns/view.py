import typing
from ..default.view import detach, View, aerosol_views, aerosol_public
from ..default.aerosol.maap import MAAP5012Optical, MAAP5012Status
from ..default.aerosol.editing.maap import EditingMAA5012


station_views = detach(aerosol_views, aerosol_public)


station_views['aerosol-raw-maap'] = MAAP5012Optical('aerosol-raw')
station_views['aerosol-realtime-maap'] = MAAP5012Optical('aerosol-realtime', realtime=True)
station_views['aerosol-editing-maap'] = EditingMAA5012('aerosol')
station_views['aerosol-clean-maap'] = MAAP5012Optical('aerosol-clean')
station_views['aerosol-avgh-maap'] = MAAP5012Optical('aerosol-avgh')
station_views['aerosol-raw-maapstatus'] = MAAP5012Status('aerosol-raw')
station_views['aerosol-realtime-maapstatus'] = MAAP5012Status('aerosol-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
