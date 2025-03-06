import asyncio
import typing
import starlette.status
import time
from json import dumps as to_json
from starlette.authentication import requires
from starlette.responses import Response, StreamingResponse
from starlette.requests import Request
from starlette.exceptions import HTTPException
from forge.const import STATIONS
from forge.formattime import format_export_time
from forge.vis.data.stream import DataStream
from forge.vis.station.lookup import station_data
from .permissions import is_available


def _get_export_bounds(request: Request):
    start_epoch_ms = 1000
    try:
        start_epoch_ms = int(request.query_params.get('start'))
    except (ValueError, TypeError):
        pass
    end_epoch_ms =  int(time.time() * 1000)
    try:
        end_epoch_ms = int(request.query_params.get('end'))
    except (ValueError, TypeError):
        pass
    return start_epoch_ms, end_epoch_ms


def _export_stream(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
                   send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    if mode_name.startswith('example-'):
        from forge.vis.data.example import ExampleEventLog
        return ExampleEventLog(start_epoch_ms, send)
    return station_data(station, 'eventlog', 'get')(station, mode_name, start_epoch_ms, end_epoch_ms, send)


@requires('authenticated')
async def export_csv(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    mode_name = request.path_params['mode_name'].lower()
    if not is_available(request, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Mode not available")
    start_epoch_ms, end_epoch_ms = _get_export_bounds(request)
    include_acquisition = 'acquisition' in request.query_params

    queue = asyncio.Queue(maxsize=256)

    def format_time(ts: int) -> str:
        if not ts:
            return ""
        return format_export_time(ts / 1000.0)

    def sanitize_field(s: str) -> str:
        if not s:
            return ""
        s = s.replace("\n", ' ')
        s = s.replace("\r", ' ')
        if ',' not in s and '"' not in s:
            return s
        s = s.replace('"', "'")
        return '"' + s + '"'

    async def send(contents: typing.Dict):
        if not include_acquisition and contents.get('acquisition'):
            return

        fields = [
            format_time(contents.get('epoch_ms')),
            sanitize_field(contents['author']),
            sanitize_field(contents['message']),
        ]
        if include_acquisition:
            fields.append(contents['type'])

        await queue.put(",".join(fields) + "\n")

    async def run(stream: DataStream):
        async def ignore_stall(reason: typing.Optional[str]):
            pass
        await stream.begin(ignore_stall)
        await stream.run()
        await queue.put(None)

    async def result(stream: DataStream):
        task = asyncio.ensure_future(run(stream))

        fields = [
            "EventTimeUTC",
            "Author",
            "Message",
        ]
        if include_acquisition:
            fields.append("Type")
        yield ",".join(fields) + "\n"

        while True:
            record = await queue.get()
            if not record:
                break
            yield record
        await task

    stream = _export_stream(station, mode_name, start_epoch_ms, end_epoch_ms, send)
    if stream is None:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Not available")
    return StreamingResponse(result(stream), media_type='text/csv', headers={
        'Content-Disposition': f'attachment; filename="{station}_log.csv"'
    })


@requires('authenticated')
async def export_json(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    mode_name = request.path_params['mode_name'].lower()
    if not is_available(request, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Mode not available")
    start_epoch_ms, end_epoch_ms = _get_export_bounds(request)
    include_acquisition = 'acquisition' in request.query_params

    queue = asyncio.Queue(maxsize=256)

    async def send(contents: typing.Dict):
        if not include_acquisition and contents.get('acquisition'):
            return
        await queue.put(to_json(contents))

    async def run(stream: DataStream):
        async def ignore_stall(reason: typing.Optional[str]):
            pass
        await stream.begin(ignore_stall)
        await stream.run()
        await queue.put(None)

    async def result(stream: DataStream):
        task = asyncio.ensure_future(run(stream))
        yield '['
        first = True
        while True:
            record = await queue.get()
            if not record:
                break
            if not first:
                yield ",\n"
            first = False
            yield record
        yield ']'
        await task

    stream = _export_stream(station, mode_name, start_epoch_ms, end_epoch_ms, send)
    if stream is None:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Not available")
    return StreamingResponse(result(stream), media_type='application/json', headers={
        'Content-Disposition': f'attachment; filename="{station}_log.json"'
    })
