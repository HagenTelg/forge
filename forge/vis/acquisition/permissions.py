from starlette.requests import Request


def is_display_available(request: Request, station: str, display_type: str, source: str):
    if display_type.startswith("example-"):
        return True
    if not request.user.allow_mode(station, 'acquisition'):
        return False
    return True


def is_summary_available(request: Request, station: str, summary_type: str, source: str):
    if summary_type.startswith("example-"):
        return True
    if not request.user.allow_mode(station, 'acquisition'):
        return False
    return True


def is_writable(request: Request, station: str):
    return request.user.allow_mode(station, 'acquisition', write=True)
