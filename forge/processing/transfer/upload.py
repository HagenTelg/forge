#!/usr/bin/env python3

import typing
import argparse
import asyncio
import aiohttp
import logging
import time
import shutil
import zstandard
from pathlib import Path
from hashlib import sha512
from tempfile import TemporaryFile
from ftplib import FTP
from base64 import b64encode, b64decode
from os.path import exists as file_exists
from starlette.datastructures import URL, QueryParams
from forge.authsocket import PublicKey, PrivateKey, key_to_bytes
from forge.processing.transfer import CONFIGURATION
from forge.processing.transfer.completion import completion_directory
from forge.processing.transfer.files import upload_ftp, upload_sftp
from forge.dashboard.report import report_ok, report_failed


_LOGGER = logging.getLogger(__name__)


class UploadRejected(Exception):
    pass


async def upload_post(session: aiohttp.ClientSession, url: URL,
                      file: Path, contents: typing.BinaryIO, compression: str,
                      public_key: PublicKey, signature: bytes) -> None:
    public_key = b64encode(key_to_bytes(public_key)).decode('ascii')
    signature = b64encode(signature).decode('ascii')

    content_type = 'application/octet-stream'
    if compression == 'zstd':
        content_type = 'application/zstd'

    upload_filename = file.name
    upload_filename.replace('"', "_")

    async with session.post(str(url), data=contents, headers={
        'X-HostID': f'{public_key} {signature}',
        'Content-Disposition': f'attachment; filename="{upload_filename}"',
        'Content-Type': content_type,
    }) as resp:
        if resp.status != 200:
            data = (await resp.read()).decode('utf-8')
            raise Exception(f"Upload not accepted by the server: {resp.reason} - {data}")
        content = await resp.json()
        status = content['status']
        if status == 'unauthorized':
            _LOGGER.debug("Authorization rejected")
            raise UploadRejected("Upload authorization rejected")
        if status == 'no_receiver':
            _LOGGER.debug("Server has no receiver target")
            raise UploadRejected("No receiver for file")
        if status != 'ok':
            _LOGGER.debug("Server storage error")
            raise UploadRejected(f"Error storing file")


async def upload_to_url(url: URL, file: Path, contents: typing.BinaryIO, compression: str,
                        public_key: PublicKey, signature: bytes) -> None:
    _LOGGER.debug(f"Uploading file to {repr(url)}")
    if url.scheme == 'ftp':
        if compression == 'zstd':
            file = file.with_suffix(file.suffix + '.zst')
        with FTP(timeout=180) as ftp:
            ftp.connect(host=url.hostname, port=url.port or 21)
            ftp.login(user=url.username or "anonymous", passwd=url.password or "anonymous")
            if url.path:
                ftp.cwd(url.path)
            upload_ftp(ftp, file, contents, public_key, signature)
    elif url.scheme == 'sftp':
        if compression == 'zstd':
            file = file.with_suffix(file.suffix + '.zst')
        params = QueryParams(url.query)
        await upload_sftp(
            url.path, file, contents, public_key, signature,
            url.hostname, url.port or 22,
            username=url.username or None, password=url.password or None,
            key_file=params.get('key', None), key_passphrase=params.get('key-passphrase', None)
        )
    elif url.scheme == 'http' or url.scheme == 'https':
        timeout = aiohttp.ClientTimeout(total=180)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            await upload_post(session, url, file, contents, compression, public_key, signature)
    else:
        raise ValueError("Unsupported URL scheme")


async def prepare_file(file: Path, private_key: PrivateKey, compression: str) -> typing.Tuple[bytes, typing.BinaryIO, int]:
    if compression == 'zstd':
        with file.open('rb') as input_file:
            upload_file: typing.BinaryIO = TemporaryFile()
            zstandard.ZstdCompressor().copy_stream(input_file, upload_file, size=file.stat().st_size)
    else:
        upload_file: typing.BinaryIO = file.open('rb')

    upload_file.seek(0)
    hasher = sha512()
    total_size = 0
    while True:
        chunk = upload_file.read(65536)
        if not chunk:
            break
        hasher.update(chunk)
        total_size += len(chunk)

    upload_file.seek(0)
    signature = private_key.sign(hasher.digest())
    return signature, upload_file, total_size


def accept_file(file: Path, args) -> bool:
    st = file.stat()
    if st.st_size == 0:
        _LOGGER.warning(f"File {file} is empty, skipping")
        return False
    if args.modified:
        now = time.time()
        if (now - st.st_mtime) < args.modified:
            _LOGGER.warning(f"File {file} modified recently, skipping")
            return False
    return True


def parse_arguments():
    parser = argparse.ArgumentParser(description="Forge data transfer uploader.")

    parser.add_argument('--key',
                        dest='key',
                        help="signing key file")
    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")

    parser.add_argument('--server',
                        help="upload server URL",
                        action='append')

    parser.add_argument('--station',
                        dest='station', type=str,
                        default=CONFIGURATION.get("ACQUISITION.STATION", 'nil').lower(),
                        help="station code")

    parser.add_argument('--dashboard',
                        dest='dashboard', type=str,
                        help="dashboard notification code")

    parser.add_argument('--modified',
                        dest='modified', type=float,
                        help="minimum time ago in seconds to accept files")

    parser.add_argument('--compression',
                        dest='compression',
                        default='zstd',
                        choices=['none', 'zstd'],
                        help="compression before upload")
    parser.add_argument('--type',
                        dest='type',
                        default='data',
                        choices=['data', 'backup', 'auxiliary'],
                        help="upload data type")
    parser.add_argument('--skip-station',
                        dest='skip_station',
                        help="station code to skip uploading for",
                        action='append')

    parser.add_argument('--completed',
                        dest='completed',
                        help="directory to move completed files to")

    parser.add_argument('--directory',
                        dest='directory',
                        help="directory to upload all files in",
                        action='append')
    parser.add_argument('file',
                        help="file to upload",
                        nargs='*')

    args = parser.parse_args()

    if not args.directory and not args.file:
        parser.error("At least one directory or file is required")

    return args


def main():
    args = parse_arguments()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()
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

    upload_files: typing.List[Path] = list()
    if args.file:
        for file in args.file:
            file = Path(file)
            if not file.exists():
                _LOGGER.fatal(f"Upload file {file} not found")
                continue
            if not accept_file(file, args):
                continue
            upload_files.append(file)
    if args.directory:
        for dir in args.directory:
            dir = Path(dir)
            if not dir.is_dir():
                _LOGGER.fatal(f"Upload directory {dir} not found")
                continue
            for file in dir.iterdir():
                if not file.is_file():
                    _LOGGER.debug(f"Skipping non-file {file}")
                    continue
                if not accept_file(file, args):
                    continue
                upload_files.append(file)

    if not upload_files:
        _LOGGER.info("No files to upload, exiting")
        exit(7)
        return

    servers = args.server
    if not servers:
        servers = CONFIGURATION.get('PROCESSING.UPLOAD.URL')
        if isinstance(servers, str):
            servers = [servers]
        elif not isinstance(servers, list):
            servers = None
    if not servers:
        _LOGGER.error("No server to upload to")
        exit(1)
        return

    _LOGGER.info(f"Starting upload of {len(upload_files)} file(s)")

    if args.skip_station and args.station in args.skip_station:
        for file in upload_files:
            _LOGGER.info(f"Skipping upload of {file} due to ignored station")
            if args.completed:
                target_dir = completion_directory(args.completed, None, args.station, args.type)
                target_dir = Path(target_dir)
                target_dir.mkdir(parents=True, exist_ok=True)
                target_file = target_dir / file.name
                try:
                    shutil.move(str(file), str(target_file))
                except IOError:
                    _LOGGER.warning(f"Failed to move {file} to completed location {target_file}", exc_info=True)
            continue
        exit(0)
        return

    upload_key = key.public_key()

    async def run():
        for file in upload_files:
            begin_time = time.monotonic()
            signature, contents, file_size = await prepare_file(file, key, args.compression)

            for url in servers:
                url = URL(url=url)
                if '{file}' in url.path:
                    url = url.replace(path=url.path.replace('{file}', file.name))
                if '{station}' in url.path:
                    url = url.replace(path=url.path.replace('{station}', args.station))
                if '{type}' in url.path:
                    url = url.replace(path=url.path.replace('{type}', args.type))

                try:
                    await upload_to_url(url, file, contents, args.compression, upload_key, signature)
                except UploadRejected:
                    _LOGGER.error(f"Upload of {file} rejected, further attempts aborted")
                    contents.close()

                    if args.dashboard:
                        await report_failed(args.dashboard, args.station, notifications=[{
                            "code": "",
                            "severity": "error",
                            "data": "File upload not authorized."
                        }])

                    exit(4)
                    return
                except:
                    _LOGGER.warning(f"Upload to {repr(url)} failed", exc_info=True)
                    continue

                break
            else:
                _LOGGER.error(f"Upload of {file} failed")
                contents.close()

                if args.dashboard:
                    await report_failed(args.dashboard, args.station, notifications=[{
                        "code": "",
                        "severity": "error",
                        "data": "Unable to reach any data upload destination servers."
                    }])

                exit(1)
                return

            contents.close()

            if args.completed:
                target_dir = completion_directory(args.completed, upload_key, args.station, args.type)
                target_dir = Path(target_dir)
                target_dir.mkdir(parents=True, exist_ok=True)
                target_file = target_dir / file.name
                try:
                    await asyncio.get_event_loop().run_in_executor(None, shutil.move, str(file), str(target_file))
                except IOError:
                    _LOGGER.warning(f"Failed to move {file} to completed location {target_file}", exc_info=True)

            if args.dashboard:
                elapsed = time.monotonic() - begin_time
                await report_ok(args.dashboard, args.station, events=[{
                    "code": "file-processed",
                    "severity": "info",
                    "data": f"{file.name},{file_size},{int(elapsed*1000)}"
                }])

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.close()


if __name__ == '__main__':
    main()
