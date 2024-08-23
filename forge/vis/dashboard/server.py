import typing
import starlette.status
from starlette.routing import Route, BaseRoute, WebSocketRoute
from starlette.datastructures import URL
from starlette.authentication import requires
from starlette.endpoints import WebSocketEndpoint
from starlette.websockets import WebSocket
from starlette.responses import Response, HTMLResponse, JSONResponse
from starlette.requests import Request
from starlette.exceptions import HTTPException
from starlette.types import ASGIApp, Receive, Scope, Send
from forge.vis.util import package_template
from forge.vis.access.database import AccessLayer as DatabaseAccess, SubscriptionLevel
from forge.dashboard import is_valid_station, is_valid_code
from forge.dashboard.display import DisplayInterface
from forge.telemetry.display import DisplayInterface as TelemetryInterface
from forge.processing.control.display import DisplayInterface as ProcessingInterface
from .permissions import dashboard_accessible, is_available
from .assemble import list_entries, get_record
from .status import Status


class _DashboardSocket(WebSocketEndpoint):
    encoding = 'json'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._is_example: bool = False
        self.db: DisplayInterface = None
        self.telemetry: typing.Optional[TelemetryInterface] = None
        self.processing: typing.Optional[ProcessingInterface] = None

    @requires('authenticated')
    async def on_connect(self, websocket: WebSocket):
        origin = websocket.headers.get('origin')
        if origin is not None and len(origin) > 0:
            origin = URL(url=origin)
            if origin.netloc != websocket.url.netloc:
                raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Cross origin disallowed")

        self._is_example = websocket.query_params.get('example') is not None

        if not self._is_example:
            if not dashboard_accessible(websocket.user):
                raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Not available")

        self.db = websocket.scope['dashboard']
        self.telemetry = websocket.scope.get('telemetry')
        self.processing = websocket.scope.get('processing')

        await websocket.accept()

    async def _get_status(self, websocket: WebSocket, station: typing.Optional[str], code: str,
                          start_epoch_ms: int) -> typing.Optional[Status]:
        if station is not None and not is_valid_station(station):
            return None
        if not is_valid_code(code):
            return None
        if self._is_example:
            if not code.startswith('example-'):
                return None
        else:
            if not is_available(websocket.user, station, code):
                return None

        record = get_record(station, code)
        if record is None:
            return None

        email = SubscriptionLevel.OFF
        if self._is_example:
            email = SubscriptionLevel.OFF
        else:
            database_layer = websocket.user.layer_type(DatabaseAccess)
            if database_layer:
                email = await database_layer.controller.get_dashboard_email(database_layer.auth_user,
                                                                            station, code)
                if email is None:
                    email = SubscriptionLevel.OFF

        return await record.status(db=self.db, telemetry=self.telemetry, processing=self.processing,
                                   email=email, station=station, entry_code=code, start_epoch_ms=start_epoch_ms)

    @requires('authenticated')
    async def on_receive(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]):
        message_type = data['type']
        if message_type == 'list':
            if not self._is_example:
                entries = await list_entries(self.db, self.telemetry, self.processing, websocket.user)
            else:
                from .example import example_list
                entries = example_list

            await websocket.send_json({
                'type': 'list',
                'entries': [entry.to_status() for entry in entries],
            })
        elif message_type == 'status':
            station = data.get('station')
            if not station:
                station = None
            else:
                station = station.lower()
            code = data['code'].lower()
            start_epoch_ms = int(data['start_epoch_ms'])

            status = await self._get_status(websocket, station, code, start_epoch_ms)
            await websocket.send_json({
                'type': 'status',
                'station': station if station else '',
                'code': code,
                'status': status.to_status() if status is not None else None,
            })


@requires('authenticated')
async def _root(request: Request) -> Response:
    is_example = request.query_params.get('example') is not None

    if not is_example:
        if not dashboard_accessible(request.user):
            raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Not available")

    return HTMLResponse(await package_template('dashboard', 'index.html').render_async(
        request=request,
        is_example=False,
        auth_layer=request.user.layer_type(DatabaseAccess),
    ))


@requires('authenticated')
async def _example(request: Request) -> Response:
    return HTMLResponse(await package_template('dashboard', 'index.html').render_async(
        request=request,
        is_example=True,
        auth_layer=request.user.layer_type(DatabaseAccess),
    ))


@requires('authenticated')
async def _details(request: Request) -> Response:
    try:
        data = await request.json()
    except:
        data = {}

    station = request.query_params.get('station')
    if not station:
        station = data.get('station')
    if station:
        station = str(station).lower()
        if not is_valid_station(station):
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid station")

    entry_code = request.query_params.get('code')
    if not entry_code:
        entry_code = data.get('code')
    if not entry_code:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Entry code is required")
    entry_code = str(entry_code).lower()
    if not is_valid_code(entry_code):
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid entry code")

    if not entry_code.startswith('example-') and not dashboard_accessible(request.user):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Data not available")
    if not is_available(request.user, station, entry_code):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Data not available")

    try:
        start_epoch_ms = request.query_params.get('start')
        if not start_epoch_ms:
            start_epoch_ms = data.get('start')
        start_epoch_ms = int(start_epoch_ms)
    except (ValueError, TypeError):
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid start time")

    record = get_record(station, entry_code)
    if record is None:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="No display available")

    return await record.details(request,
                                db=request.scope['dashboard'],
                                telemetry=request.scope.get('telemetry'),
                                processing=request.scope.get('processing'),
                                station=station, entry_code=entry_code, start_epoch_ms=start_epoch_ms,
                                uid=request.query_params.get('uid', '_'))


@requires('authenticated')
async def _set_email(request: Request) -> Response:
    data = await request.json()

    severity = str(data.get('severity')).lower()
    if severity and severity != 'off':
        try:
            severity = SubscriptionLevel(severity)
        except ValueError:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid severity")
    else:
        severity = None

    entries = data.get('entries')
    if not entries or not isinstance(entries, list):
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid entry selection")

    db = request.scope['dashboard']

    apply_entries: typing.Set[typing.Tuple[typing.Optional[str], str]] = set()
    is_all_example = None
    for e in entries:
        station = e.get('station')
        if station:
            station = str(station).lower()
            if not is_valid_station(station):
                raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid station")
        else:
            station = None

        entry_code = e.get('code')
        if not entry_code:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Entry code is required")
        entry_code = str(entry_code).lower()
        if not is_valid_code(entry_code):
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid entry code")

        if entry_code.startswith('example-'):
            if is_all_example is not None:
                if not is_all_example:
                    raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Mixed example rejected")
            is_all_example = True
            continue
        else:
            if is_all_example is not None:
                if is_all_example:
                    raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Mixed example rejected")
            else:
                if not dashboard_accessible(request.user):
                    raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Entry not available")
            is_all_example = False

        if not is_available(request.user, station, entry_code):
            raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Entry not available")
        # Telemetry is generated on the fly, rather than a pre-existing entry, so it doesn't have a database
        # link
        if entry_code != 'acquisition-telemetry':
            if not await db.entry_exists(station, entry_code):
                raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Entry not available")

        apply_entries.add((station, entry_code))

    if is_all_example:
        return JSONResponse({'status': 'ok'})

    auth_layer = request.user.layer_type(DatabaseAccess)
    if not auth_layer:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Not using a dynamic login")

    if not apply_entries:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid entry selection")

    await auth_layer.controller.set_dashboard_email(auth_layer.auth_user, severity, apply_entries)

    return JSONResponse({'status': 'ok'})


async def _badge_json(request: Request) -> Response:
    station = request.path_params.get('station')
    if station and not is_valid_station(station):
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid station")
    entry_code = request.path_params['code']
    if not is_valid_code(entry_code):
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid entry code")

    record = get_record(station, entry_code)
    if record is None:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="No display available")

    return await record.badge_json(
        request,
        db=request.scope['dashboard'],
        telemetry=request.scope.get('telemetry'),
        processing=request.scope.get('processing'),
        station=station, entry_code=entry_code
    )


async def _badge_svg(request: Request) -> Response:
    station = request.path_params.get('station')
    if station and not is_valid_station(station):
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid station")
    entry_code = request.path_params['code']
    if not is_valid_code(entry_code):
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid entry code")

    record = get_record(station, entry_code)
    if record is None:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="No display available")

    return await record.badge_svg(
        request,
        db=request.scope['dashboard'],
        telemetry=request.scope.get('telemetry'),
        processing=request.scope.get('processing'),
        station=station, entry_code=entry_code
    )


class DatabaseMiddleware:
    def __init__(self, app: ASGIApp, database_uri: str):
        self.app = app
        self.db = DisplayInterface(database_uri)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope['dashboard'] = self.db
        await self.app(scope, receive, send)


sockets: typing.List[BaseRoute] = [
    WebSocketRoute('/', _DashboardSocket, name='dashboard_socket'),
]

routes: typing.List[Route] = [
    Route('/details', endpoint=_details, methods=['GET', 'POST'], name='dashboard_details'),
    Route('/email', endpoint=_set_email, methods=['POST'], name='dashboard_email'),
    Route('/example', endpoint=_example, name='dashboard_example'),
    Route('/badge/{code}/{station}.json', endpoint=_badge_json),
    Route('/badge/{code}/{station}.svg', endpoint=_badge_svg),
    Route('/badge/{code}.json', endpoint=_badge_json),
    Route('/badge/{code}.svg', endpoint=_badge_svg),
    Route('/', endpoint=_root, name='dashboard'),
]
