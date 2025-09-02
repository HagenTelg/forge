import typing
from starlette.routing import Route
from .passed import latest_passed, passed_modal
from .instruments import instruments_modal


routes: typing.List[Route] = [
    Route('/{station}/{mode_name}/latest_passed', endpoint=latest_passed, name='latest_passed'),
    Route('/{station}/{mode_name}/passed_modal', endpoint=passed_modal, name='passed_modal'),
    Route('/{station}/{mode_name}/instruments_modal', endpoint=instruments_modal, name='instruments_modal'),
]
