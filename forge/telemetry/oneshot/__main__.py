#!/usr/bin/python3

import typing
import argparse
import asyncio
import aiohttp
import logging
import time
from io import BytesIO
from ftplib import FTP
from secrets import token_bytes
from base64 import b64encode, b64decode, b32encode
from os.path import exists as file_exists
from json import dumps as to_json
from starlette.datastructures import URL
from forge.authsocket import WebsocketJSON as AuthSocket, PublicKey, PrivateKey, key_to_bytes
from forge.telemetry import CONFIGURATION
from forge.telemetry.assemble import complete as assemble_telemetry
from forge.telemetry.assemble.station import get_station


_LOGGER = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Forge telemetry one-shot submitter.")

    parser.add_argument('--key',
                        dest='key',
                        help="signing key file")

    default_url = CONFIGURATION.get('TELEMETRY.URL')
    if isinstance(default_url, str):
        default_url = [default_url]
    elif not isinstance(default_url, list):
        default_url = None
    parser.add_argument('server',
                        help="telemetry server URL",
                        default=default_url,
                        nargs='*')
    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")

    parser.add_argument('--time-output',
                        dest='time_output',
                        help="output time file",
                        default=CONFIGURATION.get('TELEMETRY.TIME_OUTPUT'))

    args = parser.parse_args()

    if not args.server:
        parser.error("A telemetry server URL is required")

    return args


def upload_ftp(ftp: FTP, telemetry: bytes, public_key: PublicKey, signature: bytes):
    signature_file = to_json({
        'public_key': b64encode(key_to_bytes(public_key)).decode('ascii'),
        'signature': b64encode(signature).decode('ascii'),
    }).encode('utf-8')
    uid = b32encode(token_bytes(10)).decode('ascii')
    ftp.storbinary(f'STOR telemetry_{uid}', BytesIO(telemetry))
    ftp.storbinary(f'STOR telemetry_{uid}.sig', BytesIO(signature_file))


async def upload_post(session: aiohttp.ClientSession, url: URL, telemetry: bytes,
                      public_key: PublicKey, signature: bytes) -> typing.Dict[str, typing.Any]:
    public_key = b64encode(key_to_bytes(public_key)).decode('ascii')
    signature = b64encode(signature).decode('ascii')
    async with session.post(str(url), data=telemetry, headers={
        'X-HostID': f'{public_key} {signature}',
        'Content-Type': 'application/json',
    }) as resp:
        if resp.status != 200:
            data = (await resp.read()).decode('utf-8')
            raise Exception(f"Telemetry not accepted by the server: {resp.reason} - {data}")
        content = await resp.json()
        if content['status'] != 'ok':
            raise Exception("Invalid telemetry response")
        return content


async def submit_to_url(url: URL, telemetry: bytes, public_key: PublicKey,
                        signature: bytes) -> typing.Optional[typing.Dict[str, typing.Any]]:
    _LOGGER.debug(f"Uploading telemetry to {repr(url)}")
    if url.scheme == 'ftp':
        with FTP(user=url.username or "anonymous", passwd=url.password or "anonymous", timeout=120) as ftp:
            ftp.connect(host=url.hostname, port=url.port or 21)
            ftp.login()
            ftp.cwd(url.path)
            upload_ftp(ftp, telemetry, public_key, signature)
        return None
    elif url.scheme == 'http' or url.scheme == 'https':
        timeout = aiohttp.ClientTimeout(total=120)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            return await upload_post(session, url, telemetry, public_key, signature)
    else:
        raise ValueError("Unsupported URL scheme")


async def upload_websocket(websocket: "aiohttp.client.ClientWebSocketResponse", key: PrivateKey,
                           station: typing.Optional[str],
                           telemetry: typing.Dict[str, typing.Any]) -> typing.Optional[typing.Dict[str, typing.Any]]:
    await AuthSocket.client_handshake(websocket, key, extra_data={
        'station': station,
    })

    await websocket.send_json({
        'request': 'update',
        'telemetry': telemetry,
    })
    await websocket.send_json({
        'request': 'get_time',
    })

    while True:
        msg = await websocket.receive_json()
        if msg.get('response') == 'server_time':
            await websocket.close()
            return msg


def main():
    args = parse_arguments()
    if args.debug:
        root_logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)-40s %(message)s')
        handler.setFormatter(formatter)
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)
        CONFIGURATION.DEBUG = True

    key = args.key
    if key is None:
        key = CONFIGURATION.SYSTEM.KEY
    if file_exists(key):
        with open(key, 'rb') as f:
            key = f.read()
        if len(key) == 32:
            key = PrivateKey.from_private_bytes(key)
        else:
            key = PrivateKey.from_private_bytes(b64decode(key.decode('ascii').strip()))
    else:
        key = PrivateKey.from_private_bytes(b64decode(key))

    async def run():
        telemetry: typing.Dict[str, typing.Any] = await assemble_telemetry()

        station: typing.Optional[str] = await get_station()
        if station:
            telemetry['station'] = station

        telemetry['sequence_number'] = int(time.time() * 1000)
        _LOGGER.debug(f"Telemetry package ready with {len(telemetry)} keys")
        telemetry_encoded = to_json(telemetry).encode('utf-8')
        telemetry_signature = key.sign(telemetry_encoded)
        telemetry_key = key.public_key()
        _LOGGER.debug(f"Signed {len(telemetry_encoded)} bytes of telemetry data")

        for url in args.server:
            url = URL(url=url)

            try:
                if url.scheme == 'ws' or url.scheme == 'wss':
                    _LOGGER.debug(f"Sending telemetry to websocket {repr(url)}")

                    if url.scheme == 'wss':
                        url = url.replace(scheme='https')
                    elif url.scheme == 'ws':
                        url = url.replace(scheme='http')
                    timeout = aiohttp.ClientTimeout(total=120)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.ws_connect(str(url)) as websocket:
                            response = await upload_websocket(websocket, key, station, telemetry)
                else:
                    response = await submit_to_url(url, telemetry_encoded,
                                                   telemetry_key, telemetry_signature)
            except:
                _LOGGER.warning(f"Upload to {repr(url)} failed", exc_info=True)
                continue

            if response and args.time_output is not None:
                try:
                    server_time = int(response.get('server_time'))
                    local_time = round(time.time())

                    _LOGGER.debug(f"Writing time file {args.time_output} with local time {local_time} and server time {server_time}")
                    with open(args.time_output, 'w') as f:
                        f.write(to_json({
                            'server_time': server_time,
                            'local_time': local_time,
                        }))
                except (ValueError, TypeError):
                    pass

            return True

        _LOGGER.error("All upload URLs failed")
        return False

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    ok = loop.run_until_complete(run())
    loop.close()

    if not ok:
        exit(1)


if __name__ == '__main__':
    main()
