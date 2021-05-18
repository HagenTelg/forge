from starlette.requests import Request
from forge.vis.station.lookup import station_data


def is_available(request: Request, station: str, view_name: str):
    if view_name.startswith("example-"):
        return True
    for mode_name in station_data(station, 'view', 'modes')(station, view_name):
        if request.user.allow_mode(station, mode_name):
            return True
    return False
