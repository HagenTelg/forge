import typing
from starlette.routing import Route
from .passed import latest_passed


routes: typing.List[Route] = [
    Route('/{station}/{mode_name}/latest_passed', endpoint=latest_passed, name='latest_passed'),
]
