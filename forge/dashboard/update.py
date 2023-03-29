import typing
import asyncio
import logging
import ipaddress
import starlette.status
from starlette.websockets import WebSocket
from forge.authsocket import WebsocketJSON as AuthSocket
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette.exceptions import HTTPException
from .storage import DashboardInterface, is_valid_code, is_valid_station
from .permissions import check_address, check_key, check_bearer
from .report.action import DashboardAction


_LOGGER = logging.getLogger(__name__)


async def _perform_update(db: DashboardInterface, station: typing.Optional[str], entry_code: str,
                          payload: typing.Dict[str, typing.Any]) -> None:
    try:
        action = DashboardAction(station, entry_code)

        if 'update_time' in payload:
            action.update_time = int(payload['update_time'])
        if 'unbounded_time' in payload:
            action.unbounded_time = bool(payload['unbounded_time'])
        if 'status' in payload:
            status = payload['status']
            if status == 'ok':
                action.failed = False
            else:
                action.failed = True

        def read_information(raw: typing.Dict[str, typing.Any]) -> typing.Tuple[str, DashboardAction.Severity, typing.Optional[str]]:
            return str(raw['code']), DashboardAction.Severity(raw['severity']), raw.get('data')

        def read_codes(raw: typing.Iterable[str], allow_empty: bool = False) -> typing.Set[str]:
            result: typing.Set[str] = set()
            for s in raw:
                s = str(s)
                if not s and allow_empty:
                    s = ''
                elif not is_valid_code(s):
                    raise ValueError(f"invalid code {s}")
                result.add(s)
            return result

        if 'notifications' in payload:
            action.notifications = [DashboardAction.Notification(*read_information(r))
                                    for r in payload['notifications']]
        if 'clear_notifications' in payload:
            action.clear_notifications = read_codes(payload['clear_notifications'], allow_empty=True)

        def read_watchdog(raw: typing.Dict[str, typing.Any]) -> DashboardAction.Watchdog:
            last_seen = None
            if 'last_seen' in raw:
                last_seen = int(raw['last_seen'])
            return DashboardAction.Watchdog(*read_information(raw), last_seen)

        if 'watchdogs' in payload:
            action.watchdogs = [read_watchdog(r) for r in payload['watchdogs']]
        if 'clear_watchdogs' in payload:
            action.clear_watchdogs = read_codes(payload['clear_watchdogs'])

        def read_event(raw: typing.Dict[str, typing.Any]) -> DashboardAction.Event:
            occurred_at = None
            if 'occurred_at' in raw:
                occurred_at = int(raw['occurred_at'])
            return DashboardAction.Event(*read_information(raw), occurred_at)

        if 'events' in payload:
            action.events = [read_event(r) for r in payload['events']]

        def read_condition(raw: typing.Dict[str, typing.Any]) -> DashboardAction.Condition:
            start_time = None
            if 'start_time' in raw:
                start_time = int(raw['start_time'])
            end_time = None
            if 'end_time' in raw:
                end_time = int(raw['end_time'])
            return DashboardAction.Condition(*read_information(raw), start_time, end_time)

        if 'conditions' in payload:
            action.conditions = [read_condition(r) for r in payload['conditions']]
    except (ValueError, TypeError, KeyError) as e:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid update") from e

    await db.apply_action(action)


class DashboardSocket(AuthSocket):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db: DashboardInterface = None
        self.origin: typing.Optional[typing.Union[ipaddress.IPv4Address, ipaddress.IPv6Address]] = None

    async def handshake(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]) -> bool:
        self.db = websocket.scope['dashboard']

        try:
            self.origin = ipaddress.ip_address(websocket.client.host)
        except ValueError:
            self.origin = None

        return True

    async def websocket_data(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]) -> None:
        station = data.get('station')
        if not station:
            station = None
        elif not is_valid_station(station):
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid station")
        entry_code = data.get('code')
        if not is_valid_code(entry_code):
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid entry code")

        if entry_code.startswith('example-'):
            await websocket.send_json({'status': 'ok'})
            return

        async def is_allowed():
            if self.origin and check_address(self.origin, station, entry_code):
                return True
            return await check_key(self.db, self.public_key, station, entry_code)

        if not is_allowed():
            raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Entry access denied")

        await _perform_update(self.db, station, entry_code, data)
        await websocket.send_json({'status': 'ok'})


async def update(request: Request) -> Response:
    data = await request.json()
    db = request.scope['dashboard']

    bearer_token: typing.Optional[str] = None
    auth = request.headers.get('Authorization')
    if auth:
        auth = auth.split()
        if len(auth) >= 2 and auth[0].lower() == 'bearer':
            bearer_token = auth[1][:64]

    station = data.get('station')
    if not station:
        station = None
    elif not is_valid_station(station):
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid station")
    entry_code = data.get('code')
    if not is_valid_code(entry_code):
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid entry code")

    if entry_code.startswith('example-'):
        return JSONResponse({'status': 'ok'})

    try:
        origin = ipaddress.ip_address(request.client.host)
    except ValueError:
        origin = None

    async def is_allowed():
        if origin and check_address(origin, station, entry_code):
            return True
        if bearer_token and await check_bearer(db, bearer_token, station, entry_code):
            return True
        return False

    if not await is_allowed():
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Entry access denied")

    await _perform_update(db, station, entry_code, data)
    return JSONResponse({'status': 'ok'})
