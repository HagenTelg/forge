import typing
import starlette.status
from secrets import token_urlsafe
from starlette.applications import Starlette
from starlette.routing import Route, Mount, NoMatchFound
from starlette.requests import Request
from starlette.responses import Response, HTMLResponse
from starlette.exceptions import HTTPException
from starlette.staticfiles import StaticFiles, FileResponse, RedirectResponse
from starlette.datastructures import Secret
from starlette.middleware import Middleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from forge.const import STATIONS
from . import CONFIGURATION
from .util import package_data, package_template, TEMPLATE_ENV
import forge.vis.access.authentication
import forge.vis.view.server
import forge.vis.mode.server
import forge.vis.data.server
import forge.vis.editing.server
import forge.vis.eventlog.server
import forge.vis.status.server
import forge.vis.export.server


async def _favicon(request: Request) -> Response:
    return FileResponse(package_data('static/favicon.png'))


async def _root(request: Request) -> Response:
    if not request.user.is_authenticated:
        try:
            return RedirectResponse(request.url_for('login'))
        except NoMatchFound:
            raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Invalid authentication")

    default_station = request.query_params.get('station')
    visible_stations = request.user.visible_stations

    if len(visible_stations) == 0:
        if not request.user.can_request_access:
            try:
                return RedirectResponse(request.url_for('login'))
            except NoMatchFound:
                raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Invalid authentication")
        if default_station is not None:
            default_station = default_station.lower()
            if default_station in STATIONS:
                return RedirectResponse(request.url_for('request_access') + f"?station={default_station}")
        return RedirectResponse(request.url_for('request_access'))

    if default_station is not None:
        default_station = default_station.lower()
        if default_station not in visible_stations:
            default_station = None
    if not default_station:
        default_station = visible_stations[0]
    return HTMLResponse(await package_template('index.html').render_async(
        request=request,
        visible_stations=visible_stations,
        default_station=default_station,
    ))


TEMPLATE_ENV.globals["ENABLE_LOGIN"] = forge.vis.access.authentication.enable_login


routes = [
    Mount('/static', app=StaticFiles(directory=package_data('static')), name='static'),
    Route('/favicon.png', endpoint=_favicon),
    Route('/favicon.ico', endpoint=_favicon),

    Mount('/auth', routes=forge.vis.access.authentication.routes),
    Mount('/view', routes=forge.vis.view.server.routes),
    Mount('/editing', routes=forge.vis.editing.server.routes),
    Mount('/eventlog', routes=forge.vis.eventlog.server.routes),
    Mount('/status', routes=forge.vis.status.server.routes),
    Mount('/export', routes=forge.vis.export.server.routes),
    Mount('/station', routes=forge.vis.mode.server.routes),
    Route('/settings', endpoint=forge.vis.mode.server.local_settings, name='local_settings'),

    Mount('/socket/data', routes=forge.vis.data.server.sockets),
    Mount('/socket/export', routes=forge.vis.export.server.sockets),

    Route('/index.html', endpoint=_root),
    Route('/index.htm', endpoint=_root),
    Route('/', endpoint=_root, name='root'),
]

middleware: typing.List[Middleware] = list()

middleware.append(Middleware(TrustedHostMiddleware,
                             allowed_hosts=CONFIGURATION.get('AUTHENTICATION.TRUSTED_HOSTS', ["*"])))

ratelimit_url = CONFIGURATION.get('RATELIMIT.REDIS')
if ratelimit_url is not None:
    from ratelimit import RateLimitMiddleware, Rule
    from ratelimit.auths.ip import client_ip
    from ratelimit.backends.redis import RedisBackend, StrictRedis, DECREASE_SCRIPT

    class Backend(RedisBackend):
        # noinspection PyMissingConstructor
        def __init__(self, url: str):
            self._redis = StrictRedis.from_url(url)
            self.decrease_function = self._redis.register_script(DECREASE_SCRIPT)

    backend = Backend(ratelimit_url)
    config = {
        r'^/auth/password/login': [Rule(minute=10)],
        r'^/auth/password/reset_issue': [Rule(minute=2, hour=20)],
        r'^/auth/password/reset': [Rule(minute=5)],
        r'^/auth/password/create': [Rule(minute=2)],
        r'^/auth/(google|microsoft|yahoo|apple)': [Rule(minute=20)],
        r'^/auth/request': [Rule(minute=10, hour=50)],
    }
    middleware.append(Middleware(RateLimitMiddleware, backend=backend, authenticate=client_ip, config=config))

middleware.append(Middleware(SessionMiddleware, session_cookie="forge_session",
                             secret_key=Secret(CONFIGURATION.get('SESSION.SECRET', token_urlsafe(32)))))
middleware.append(Middleware(AuthenticationMiddleware, backend=forge.vis.access.authentication.AuthBackend()))

app = Starlette(routes=routes, middleware=middleware)
