import asyncio
import typing
import logging
import starlette.status
from tempfile import TemporaryFile
from cgi import parse_header
from base64 import b64decode, urlsafe_b64decode, b64encode
from hashlib import sha512
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette.exceptions import HTTPException
from cryptography.exceptions import InvalidSignature
from forge.const import STATIONS
from forge.authsocket import PublicKey, key_to_bytes
from .. import CONFIGURATION
from ..storage.client import WriteFile, FileType, Compression

_LOGGER = logging.getLogger(__name__)
_DIRECTORY = CONFIGURATION.get('PROCESSING.INCOMING.DIRECTORY', '/var/tmp')
_MAXIMUM_SIZE = CONFIGURATION.get('PROCESSING.INCOMING.MAXIMUM_SIZE', 100 * 1024 * 1024)


async def _begin_receive(request: Request) -> typing.Tuple[bytes, WriteFile]:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")

    id_header = request.headers.get('X-HostID')
    if not id_header:
        try:
            key = PublicKey.from_public_bytes(urlsafe_b64decode(request.query_params['publickey']))
        except:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST,
                                detail="Invalid verification key")
        try:
            signature = urlsafe_b64decode(request.query_params['signature'])
        except:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid signature")
    else:
        id_header = id_header.split()
        if len(id_header) != 2:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST,
                                detail="Invalid host identification header")
        try:
            key = PublicKey.from_public_bytes(b64decode(id_header[0].strip()))
        except:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST,
                                detail="Invalid verification key")
        try:
            signature = b64decode(id_header[1].strip())
        except:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid signature")

    content_disposition = request.headers.get('Content-Disposition')
    if not content_disposition:
        try:
            filename = request.query_params['filename']
        except:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid file name")
    else:
        _, params = parse_header(content_disposition)
        filename = params.get('filename')
    if not filename:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="No file name provided")

    writer = WriteFile(station, key, filename)

    content_type = request.headers.get('Content-Type')
    if content_type and content_type.lower() == 'application/zstd':
        writer.compression = Compression.ZSTD

    return signature, writer


async def _receive_file(request: Request, output: WriteFile, signature: bytes) -> Response:
    hasher = sha512()

    def hash_chunk(chunk: bytes) -> None:
        hasher.update(chunk)

    total_size = 0
    with TemporaryFile(dir=_DIRECTORY) as storage:
        _LOGGER.debug(f"Receiving {output.file_type.name} {output.station.upper()}/{output.filename}")

        loop = asyncio.get_running_loop()
        async for chunk in request.stream():
            await asyncio.wait([
                asyncio.ensure_future(loop.run_in_executor(None, storage.write, chunk)),
                asyncio.ensure_future(loop.run_in_executor(None, hash_chunk, chunk)),
            ])

            total_size += len(chunk)
            if total_size > _MAXIMUM_SIZE:
                raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="File too large")
        try:
            output.key.verify(signature, hasher.digest())
        except InvalidSignature:
            raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Error verifying content")

        reader, writer = await asyncio.open_unix_connection(
            CONFIGURATION.get('PROCESSING.TRANSFER.SOCKET', '/run/forge-transfer-storage.socket'))

        if not await output.connect(reader, writer):
            return JSONResponse({
                'status': 'no_receiver',
            })

        try:
            storage.seek(0)
            while True:
                chunk = storage.read(0xFFFF)
                if not chunk:
                    break
                await output.write_chunk(chunk)

            ok = await output.complete()
            output = None
        finally:
            if output:
                await output.abort()
                ok = False

    if not ok:
        _LOGGER.debug(f"Receive finalization failed")
        return JSONResponse({
            'status': 'failed',
            'size': total_size,
        })

    _LOGGER.debug(f"Receive complete of {total_size} bytes")
    return JSONResponse({
        'status': 'ok',
        'size': total_size,
    })


async def receive_data(request: Request) -> Response:
    signature, writer = await _begin_receive(request)
    writer.file_type = FileType.DATA
    if not await request.scope['processing'].incoming_data_authorized(writer.key, writer.station):
        _LOGGER.debug(f"Data upload for {writer.station.upper()}/{writer.filename} from {b64encode(key_to_bytes(writer.key))} rejected")
        return JSONResponse({
            'status': 'unauthorized',
        })
    return await _receive_file(request, writer, signature)


async def receive_backup(request: Request) -> Response:
    signature, writer = await _begin_receive(request)
    writer.file_type = FileType.BACKUP
    if not await request.scope['processing'].incoming_backup_authorized(writer.key, writer.station):
        _LOGGER.debug(f"Backup upload for {writer.station.upper()}/{writer.filename} from {b64encode(key_to_bytes(writer.key))} rejected")
        return JSONResponse({
            'status': 'unauthorized',
        })
    return await _receive_file(request, writer, signature)


async def receive_auxiliary(request: Request) -> Response:
    signature, writer = await _begin_receive(request)
    writer.file_type = FileType.AUXILIARY
    if not await request.scope['processing'].incoming_auxiliary_authorized(writer.key, writer.station):
        _LOGGER.debug(f"Auxiliary upload for {writer.station.upper()}/{writer.filename} from {b64encode(key_to_bytes(writer.key))} rejected")
        return JSONResponse({
            'status': 'unauthorized',
        })
    return await _receive_file(request, writer, signature)
