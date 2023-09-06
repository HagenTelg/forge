import typing
from starlette.routing import Route
from starlette.authentication import AuthenticationBackend, AuthCredentials
from starlette.responses import Response, JSONResponse
from forge.vis import CONFIGURATION
from . import BaseAccessLayer, AccessUser, BaseAccessController, Request


async def _status(request: Request) -> Response:
    if not request.user.is_authenticated:
        return JSONResponse({
            'authenticated': False,
            'host': request.client.host,
        })
    return JSONResponse({
        'authenticated': True,
        'name': request.user.display_name,
        'id': request.user.display_id,
        'host': request.client.host,
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

methods.sort(key=lambda x: x.sort_order)


class AuthBackend(AuthenticationBackend):
    async def authenticate(self, request: Request):
        layers: typing.List[BaseAccessLayer] = list()
        for m in methods:
            layer = await m.authenticate(request)
            if layer is None:
                continue
            layers.append(layer)
        if layers:
            return AuthCredentials(['authenticated']), AccessUser(layers)
