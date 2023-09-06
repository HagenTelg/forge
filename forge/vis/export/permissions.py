from forge.vis.access import AccessUser


def is_available(user: AccessUser, station: str, mode_name: str):
    if mode_name.startswith("example-"):
        return True
    return user.allow_mode(station, mode_name)
