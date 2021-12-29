import typing
from ..default.view import detach, View, aerosol_views
from ..default.aerosol.pressure import BasicPressure
from ..default.aerosol.tsi377Xcpc import TSI3775CPCStatus


station_views = detach(aerosol_views)

station_views['aerosol-raw-cpcstatus'] = TSI3775CPCStatus('aerosol-raw')
station_views['aerosol-raw-pressure'] = BasicPressure('aerosol-raw')
station_views['aerosol-realtime-cpcstatus'] = TSI3775CPCStatus('aerosol-realtime', realtime=True)
station_views['aerosol-realtime-pressure'] = BasicPressure('aerosol-realtime', realtime=True)


def get(station: str, view_name: str) -> typing.Optional[View]:
    return station_views.get(view_name)
