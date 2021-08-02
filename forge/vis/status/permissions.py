from starlette.requests import Request
from forge.vis.mode.permissions import is_available as mode_available


def is_available(request: Request, station: str, mode_name: str):
    return mode_available(request, station, mode_name)

