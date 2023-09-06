from forge.vis.access import AccessUser
from forge.vis.station.lookup import station_data


def is_available(user: AccessUser, station: str, data_name: str):
    if data_name.startswith("example-"):
        return True
    for mode_name in station_data(station, 'data', 'modes')(station, data_name):
        if user.allow_mode(station, mode_name):
            return True
    return False
