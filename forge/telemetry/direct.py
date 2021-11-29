import typing
import logging
import starlette.status
import ipaddress
import time
from base64 import b64decode, urlsafe_b64decode
from json import loads as from_json
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette.exceptions import HTTPException
from cryptography.exceptions import InvalidSignature
from . import PublicKey


_LOGGER = logging.getLogger(__name__)


async def update(request: Request) -> Response:
    id_header = request.headers.get('X-HostID')
    if not id_header:
        try:
            key = PublicKey.from_public_bytes(urlsafe_b64decode(request.query_params['publickey']))
        except:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid verification key")
        try:
            signature = urlsafe_b64decode(request.query_params['signature'])
        except:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid signature")
    else:
        id_header = id_header.split()
        if len(id_header) != 2:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid host identification header")
        try:
            key = PublicKey.from_public_bytes(b64decode(id_header[0].strip()))
        except:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid verification key")
        try:
            signature = b64decode(id_header[1].strip())
        except:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid signature")

    body = await request.body()
    if not body or len(body) < 4:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="No telemetry content provided")
    if len(body) > 16 * 1024 * 1024:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Telemetry package is too large")
    try:
        key.verify(signature, body)
    except InvalidSignature:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Error verifying content")

    try:
        content = from_json(body.decode('utf-8'))
        if not isinstance(content, dict):
            raise ValueError
    except:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Error decoding json")

    try:
        origin = str(ipaddress.ip_address(request.client.host))
    except ValueError:
        origin = None

    if not await request.scope['telemetry'].direct_update(key, origin, content):
        _LOGGER.debug("Direct telemetry update rejected")
        return JSONResponse({
            'status': 'rejected',
        })

    return JSONResponse({
        'status': 'ok',
        'server_time': round(time.time()),
    })
