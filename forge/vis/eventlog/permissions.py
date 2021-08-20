from starlette.requests import Request


def is_available(request: Request, station: str, mode_name: str):
    if mode_name.startswith("example-"):
        return True
    return request.user.allow_mode(station, mode_name)

