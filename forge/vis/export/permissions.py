from forge.vis.access import BaseAccessUser


def is_available(user: BaseAccessUser, station: str, mode_name: str):
    if mode_name.startswith("example-"):
        return True
    return user.allow_mode(station, mode_name)
