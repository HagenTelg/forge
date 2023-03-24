import typing
import asyncio
import aiohttp
import logging
from os.path import exists as file_exists
from base64 import b64decode
from forge.dashboard import CONFIGURATION
from forge.authsocket import WebsocketJSON as AuthSocket, PrivateKey
from .action import DashboardAction


_LOGGER = logging.getLogger(__name__)


async def _upload_post(url: str, action: DashboardAction, bearer_token: typing.Optional[str] = None) -> None:
    headers = {
        'Content-Type': 'application/json',
    }
    if bearer_token:
        headers['Authorization'] = 'Bearer ' + bearer_token

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=action.to_json(), headers=headers) as resp:
            if resp.status != 200:
                data = (await resp.read()).decode('utf-8')
                raise Exception(f"Upload not accepted by the server: {resp.reason} - {data}")
            content = await resp.json()
            status = content['status']
            if status != 'ok':
                raise Exception(f"Upload not accepted by the server: {status}")


async def _upload_websocket(url: str, action: DashboardAction, private_key: PrivateKey) -> None:
    timeout = aiohttp.ClientTimeout(connect=30, sock_read=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.ws_connect(url) as websocket:
            await AuthSocket.client_handshake(websocket, private_key)
            await websocket.send_json(action.to_json())
            response = await websocket.receive_json()
            status = response['status']
            if status != 'ok':
                raise Exception(f"Upload not accepted by the server: {status}")


async def dashboard_report(code: str,
                           station: typing.Optional[str] = None,
                           unreported_exception: bool = False,
                           **kwargs) -> None:
    action = DashboardAction.from_args(station, code, **kwargs)

    url = kwargs.get('url') or CONFIGURATION.get('DASHBOARD.REPORT.URL')
    if not url:
        if unreported_exception:
            raise RuntimeError("No dashboard reporting URL configured")
        if action.failed:
            _LOGGER.error(f"Unable to report dashboard failure for {action.code}")
        else:
            _LOGGER.info(f"Unable to report dashboard success for {action.code}")
        return

    _LOGGER.debug(f"Sending dashboard report for {action.code} to {url}")

    if url.startswith('ws'):
        key = CONFIGURATION.get('DASHBOARD.REPORT.KEY', kwargs.get('key'))
        if key is None:
            key = CONFIGURATION.get('SYSTEM.KEY')
        if key is None:
            if unreported_exception:
                raise RuntimeError("No dashboard reporting private key configured")
            if action.failed:
                _LOGGER.error(f"No key to report dashboard failure for {action.code}")
            else:
                _LOGGER.info(f"No key to report dashboard success for {action.code}")
            return

        if file_exists(key):
            with open(key, 'rb') as f:
                key = f.read()
            if len(key) == 32:
                key = PrivateKey.from_private_bytes(key)
            else:
                key = PrivateKey.from_private_bytes(b64decode(key.decode('ascii').strip()))
        else:
            key = PrivateKey.from_private_bytes(b64decode(key))

        url = 'http' + url[2:]
        for t in range(4):
            try:
                await _upload_websocket(url, action, key)
                _LOGGER.debug(f"Dashboard report complete for {action.code}")
                return
            except:
                pass
            await asyncio.sleep(10)

        try:
            await _upload_websocket(url, action, key)
            _LOGGER.debug(f"Dashboard report complete for {action.code}")
        except:
            if unreported_exception:
                raise
            if action.failed:
                _LOGGER.error(f"Error during dashboard failure report for {action.code}", exc_info=True)
            else:
                _LOGGER.warning(f"Error during dashboard success report for {action.code}", exc_info=True)
        return

    bearer_token = CONFIGURATION.get('DASHBOARD.REPORT.BEARER_TOKEN', kwargs.get('bearer_token'))
    if bearer_token and file_exists(bearer_token):
        with open(bearer_token, 'r') as f:
            bearer_token = f.read().strip()

    for t in range(4):
        try:
            await _upload_post(url, action, bearer_token)
            _LOGGER.debug(f"Dashboard report complete for {action.code}")
            return
        except:
            pass
        await asyncio.sleep(10)

    try:
        await _upload_post(url, action, bearer_token)
        _LOGGER.debug(f"Dashboard report complete for {action.code}")
    except:
        if unreported_exception:
            raise
        if action.failed:
            _LOGGER.error(f"Error during dashboard failure report for {action.code}", exc_info=True)
        else:
            _LOGGER.warning(f"Error during dashboard success report for {action.code}", exc_info=True)


async def report_ok(code: str, station: typing.Optional[str] = None, **kwargs) -> None:
    return await dashboard_report(code, station, failed=False, **kwargs)


async def report_failed(code: str, station: typing.Optional[str] = None, **kwargs) -> None:
    return await dashboard_report(code, station, failed=True, **kwargs)
