from starlette.requests import Request


def is_available(request: Request, station: str, mode_name: str):
    if mode_name == "example-editing":
        return True
    return request.user.allow_mode(station, mode_name)


def is_writable(request: Request, station: str, mode_name: str):
    if mode_name == "example-editing":
        return True
    return request.user.allow_mode(station, mode_name, write=True)
