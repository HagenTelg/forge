from starlette.requests import Request
from forge.vis.mode.permissions import is_available as mode_available


def is_available(request: Request, station: str, mode_name: str):
    return mode_available(request, station, mode_name)


def is_writable(request: Request, station: str, mode_name: str):
    if mode_name.startswith("example-"):
        return True
    return request.user.allow_mode(station, mode_name, write=True)
