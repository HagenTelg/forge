import typing
from starlette.routing import Route
from starlette.authentication import AuthenticationBackend, AuthCredentials
from starlette.responses import Response, JSONResponse
from forge.vis import CONFIGURATION
from . import BaseAccessController, Request


async def _status(request: Request) -> Response:
    if not request.user.is_authenticated:
        return JSONResponse({'authenticated': False})
    return JSONResponse({
        'authenticated': True,
        'name': request.user.display_name,
        'id': request.user.display_id,
    })


methods: typing.List[BaseAccessController] = list()
routes: typing.List[Route] = [
    Route('/', endpoint=_status)
]

enable_login = False

if CONFIGURATION.exists('AUTHENTICATION.DATABASE'):
    from .database import AccessController as DatabaseAccessController
    controller = DatabaseAccessController(CONFIGURATION.AUTHENTICATION.DATABASE)
    methods.append(controller)
    routes.extend(controller.routes)
    enable_login = True

if CONFIGURATION.exists('AUTHENTICATION.STATIC'):
    from .address import AccessController as StaticAccessController
    static_config = CONFIGURATION.AUTHENTICATION.STATIC
    if isinstance(static_config, dict):
        methods.append(StaticAccessController(static_config))
    else:
        for c in static_config:
            methods.append(StaticAccessController(c))


class AuthBackend(AuthenticationBackend):
    async def authenticate(self, request: Request):
        for m in methods:
            user = await m.authenticate(request)
            if user is not None:
                return AuthCredentials(['authenticated']), user
