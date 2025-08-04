import typing
import asyncio
import aiohttp
import argparse
import aiohttp.typedefs
from json import JSONDecodeError, loads as from_json, dumps as to_json
from forge.acquisition import LayeredConfiguration
from starlette.datastructures import URL
from .base import BaseSimulator, BaseContext, BaseDataOutput, BasePersistentInterface, BaseBusInterface, CommunicationsError
from .standard import IterativeCommunicationsInstrument

if typing.TYPE_CHECKING:
    from starlette.routing import Route
    from starlette.applications import Starlette
    from .testing import Starlette


async def _handle_response( resp, require_ok: bool = True, json: bool = False):
    if require_ok and resp.status != 200:
        data = (await resp.read()).decode('utf-8')
        raise CommunicationsError(f"invalid response status: {resp.reason} - {data}")
    if json:
        try:
            return await resp.json()
        except JSONDecodeError as e:
            raise CommunicationsError("invalid response") from e
    return await resp.read()


class HttpContext(BaseContext):
    def __init__(self, config: LayeredConfiguration, data: BaseDataOutput, bus: BaseBusInterface,
                 persistent: BasePersistentInterface):
        super().__init__(config, data, bus, persistent)
        self.url: typing.Optional[URL] = None
        self.connector: typing.Optional[aiohttp.BaseConnector] = None
        self.timeout = aiohttp.ClientTimeout(total=30)
        self.headers: typing.Union[typing.Dict[str, str], typing.Iterable[typing.Tuple[str, str]]] = dict()

    def session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            connector=self.connector,
            timeout=self.timeout,
            headers=self.headers,
        )

    def url_path(self, path: typing.Optional[str]) -> URL:
        if not path:
            return self.url
        return self.url.replace(path=self.url.path + path)

    async def get(self, path: typing.Optional[str] = None, require_ok: bool = True, json: bool = False):
        async with self.session() as session:
            async with session.get(str(self.url_path(path))) as resp:
                return await _handle_response(resp, require_ok, json)

    async def post(
            self,
            data: typing.Union[bytes, str, dict, list],
            path: typing.Optional[str],
            require_ok: bool = True,
            json: bool = False,
    ):
        async with self.session() as session:
            args = dict()
            if isinstance(data, dict) or isinstance(data, list):
                args['json'] = data
            else:
                args['data'] = data
            async with session.post(str(self.url_path(path)), **args) as resp:
                return await _handle_response(resp, require_ok, json)


class HttpInstrument(IterativeCommunicationsInstrument):
    REQUIRE_URL: bool = True

    def __init__(self, context: HttpContext):
        super().__init__(context)
        self.context = context

    async def get(self, path: typing.Optional[str] = None, require_ok: bool = True, json: bool = False):
        return await self.context.get(path, require_ok, json)

    async def post(
            self,
            data: typing.Union[bytes, str, dict, list],
            path: typing.Optional[str],
            require_ok: bool = True,
            json: bool = False,
    ):
        return await self.context.post(data, path, require_ok, json)


class HttpSimulator(BaseSimulator):
    class TestContext(HttpContext):
        def __init__(self, config: LayeredConfiguration, data: BaseDataOutput, bus: BaseBusInterface,
                     persistent: BasePersistentInterface, routes: typing.List["Route"]):
            from starlette.routing import Router
            super().__init__(config, data, bus, persistent)
            self.router = Router(routes)
            self.url = URL("http://localhost")

        def _scope(self, method: str, path: typing.Optional[str] = None):
            u = self.url_path(path)
            return {
                "type": "http",
                "scheme": "http",
                "http_version": "1.1",
                "method": method,
                "path": u.path,
                "query_string": u.query,
                "headers": self.headers,
            }

        async def _handle_response(self, scope, body: bytes, require_ok: bool = True, json: bool = False):
            async def receive():
                return {
                    "type": "http.response.body",
                    "body": b"",
                    "more_body": False
                }

            body = bytearray()
            status = 0

            async def send(message):
                nonlocal body
                nonlocal status

                t = message.get("type")
                if t == "http.response.start":
                    status = message.get("status", 0)
                elif t == "http.response.body":
                    body += message.get("body", b"")

            await self.router(scope, receive, send)

            if require_ok and status != 200:
                raise CommunicationsError
            if json:
                return from_json(bytes(body))
            return bytes(body)

        async def get(self, path: typing.Optional[str] = None, require_ok: bool = True, json: bool = False):
            return await self._handle_response(self._scope("GET", path), b"", require_ok, json)

        async def post(
                self,
                data: typing.Union[bytes, str, dict, list],
                path: typing.Optional[str],
                require_ok: bool = True,
                json: bool = False,
        ):
            if isinstance(data, str):
                data = data.encode('utf-8')
            elif not isinstance(data, bytes):
                data = to_json(data).encode('utf-8')
            return await self._handle_response(self._scope("GET", path), data, require_ok, json)

    @property
    def routes(self) -> typing.List["Route"]:
        raise NotImplementedError

    @classmethod
    def arguments(cls) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="Forge acquisition simulated instrument.")

        group = parser.add_mutually_exclusive_group()
        group.add_argument('--bind',
                           dest="bind_address",
                           help="the IP address to listen for connections on")
        parser.add_argument('--port',
                            dest="port", type=int, default=8000,
                            help="TCP port to listen for connections on")
        group.add_argument('--unix',
                           dest="bind_unix",
                           help="Unix socket to listen for connections on")

        parser.add_argument('--debug',
                            dest='debug', action='store_true',
                            help="enable debug output")

        return parser

    @classmethod
    def construct_server(cls, cli, *args, **kwargs) -> "Starlette":
        from starlette.applications import Starlette
        simulator = cls(*args, **kwargs)
        return Starlette(routes=simulator.routes, debug=cli.debug)

    @classmethod
    def run_server(cls, *args, **kwargs) -> None:
        import uvicorn

        cli = cls.arguments()
        cli = cli.parse_args()
        if cli.debug:
            from forge.log import set_debug_logger
            set_debug_logger()

        log_level = 'info'
        log_config = uvicorn.config.LOGGING_CONFIG
        if cli.debug:
            log_config['loggers'][''] = {"handlers": ["default"], "level": "DEBUG"}
            log_config['loggers']['uvicorn']['propagate'] = False
            log_level = 'debug'

        def make_app():
            return cls.construct_server(cli, *args, **kwargs)

        if cli.bind_unix is not None:
            import os
            try:
                os.unlink(cli.bind_unix)
            except OSError:
                pass
            uvicorn.run(make_app, factory=True, workers=1, access_log=cli.debug,
                        uds=cli.bind_unix, log_level=log_level, log_config=log_config)
        elif cli.bind_address is not None:
            uvicorn.run(make_app, factory=True, workers=1, access_log=cli.debug,
                        host=cli.bind_address, port=cli.port, log_level=log_level, log_config=log_config)
        else:
            uvicorn.run(make_app, factory=True, workers=1, access_log=cli.debug,
                        port=cli.port, log_level=log_level, log_config=log_config)

    async def run(self) -> None:
        pass


def launch(instrument: typing.Type[HttpInstrument]) -> None:
    from .run import run, arguments, average_config, instrument_config, cutsize_config, \
        data_output, bus_interface, persistent_interface

    args = arguments()
    args = args.parse_args()
    bus = bus_interface(args)
    data = data_output(args)
    persistent = persistent_interface(args)
    instrument_config = instrument_config(args)

    ctx = HttpContext(instrument_config, data, bus, persistent)
    ctx.average_config = average_config(args)
    ctx.cutsize_config = cutsize_config(args)

    if instrument.REQUIRE_URL:
        url = str(instrument_config.get("URL"))
        if not url:
            raise ValueError("No instrument URL set")
        ctx.url = URL(url=url)

    headers = instrument_config.get("HEADERS")
    if isinstance(headers, dict):
        for k, v in headers.items():
            ctx.headers[k] = v
    elif isinstance(headers, list):
        ctx.headers = list()
        for sub in headers:
            if isinstance(sub, str):
                k, v = sub.split(':', 1)
                ctx.headers.append((k.strip(), v.strip()))
                continue
            for k, v in sub.items():
                ctx.headers.append((k.strip(), v.strip()))

    unix = instrument_config.get("UNIX")
    if unix:
        ctx.connector = aiohttp.UnixConnector(str(unix))

    def timeout():
        timeout = instrument_config.get("TIMEOUT")
        if timeout is None:
            return
        if not isinstance(timeout, LayeredConfiguration) and not isinstance(timeout, dict):
            if isinstance(timeout, bool) and not timeout:
                ctx.timeout = aiohttp.ClientTimeout()
            else:
                ctx.timeout = aiohttp.ClientTimeout(total=float(timeout))
            return

        def value_or_none(key: str) -> typing.Optional[float]:
            v = timeout.get(key)
            if v is None:
                return None
            return float(v)

        ctx.timeout = aiohttp.ClientTimeout(
            total=value_or_none("TOTAL"),
            connect=value_or_none("CONNECT"),
            sock_read=value_or_none("SOCK_READ"),
            sock_connect=value_or_none("SOCK_CONNECT"),
        )

    timeout()

    instrument = instrument(ctx)
    ctx.persistent.version = instrument.PERSISTENT_VERSION
    run(instrument, args.systemd)
