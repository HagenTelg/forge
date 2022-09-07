#!/usr/bin/python3

import typing
import argparse
import asyncio
import aiohttp
import logging
import time
import zstandard
from pathlib import Path
from io import BytesIO
from hashlib import sha512
from tempfile import TemporaryFile
from ftplib import FTP
from base64 import b64encode, b64decode
from os.path import exists as file_exists
from json import dumps as to_json
from shutil import move as move_file
from starlette.datastructures import URL
from forge.authsocket import PublicKey, PrivateKey, key_to_bytes
from forge.processing.transfer import CONFIGURATION
from forge.processing.transfer.completion import completion_directory


_LOGGER = logging.getLogger(__name__)


class UploadRejected(Exception):
    pass


def upload_ftp(ftp: FTP, file: Path, contents: typing.BinaryIO, public_key: PublicKey, signature: bytes):
    signature_file = to_json({
        'public_key': b64encode(key_to_bytes(public_key)).decode('ascii'),
        'signature': b64encode(signature).decode('ascii'),
    }).encode('utf-8')
    ftp.storbinary(f'STOR {file.name}', contents)
    ftp.storbinary(f'STOR {file.name}.sig', BytesIO(signature_file))


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
            raise UploadRejected(f"Upload not accepted by the server: {resp.reason} - {data}")
        content = await resp.json()
        status = content['status']
        if status == 'unauthorized':
            _LOGGER.debug("Authorization rejected for")
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
            file = file.with_suffix('zst')
        with FTP(user=url.username or "anonymous", passwd=url.password or "anonymous", timeout=180) as ftp:
            ftp.connect(host=url.hostname, port=url.port or 21)
            ftp.login()
            ftp.cwd(url.path)
            upload_ftp(ftp, file, contents, public_key, signature)
    elif url.scheme == 'http' or url.scheme == 'https':
        timeout = aiohttp.ClientTimeout(total=180)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            await upload_post(session, url, file, contents, compression, public_key, signature)
    else:
        raise ValueError("Unsupported URL scheme")


async def prepare_file(file: Path, private_key: PrivateKey, compression: str) -> typing.Tuple[bytes, typing.BinaryIO]:
    if compression == 'zstd':
        with file.open('rb') as input_file:
            upload_file: typing.BinaryIO = TemporaryFile()
            zstandard.ZstdCompressor().copy_stream(input_file, upload_file, size=file.stat().st_size)
    else:
        upload_file: typing.BinaryIO = file.open('rb')

    upload_file.seek(0)
    hasher = sha512()
    while True:
        chunk = upload_file.read(65536)
        if not chunk:
            break
        hasher.update(chunk)

    upload_file.seek(0)
    signature = private_key.sign(hasher.digest())
    return signature, upload_file


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

    default_url = CONFIGURATION.get('PROCESSING.UPLOAD.URL')
    if isinstance(default_url, str):
        default_url = [default_url]
    elif not isinstance(default_url, list):
        default_url = None
    parser.add_argument('--server',
                        help="upload server URL",
                        default=default_url,
                        nargs=default_url and '*' or '+')

    parser.add_argument('--station',
                        dest='station', type=str,
                        default=CONFIGURATION.get("ACQUISITION.STATION", 'nil'),
                        help="station code")

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
                        nargs='*')

    parser.add_argument('--completed',
                        dest='completed',
                        help="directory to move completed files to")

    parser.add_argument('--directory',
                       dest='directory',
                       help="directory to upload all files in",
                       nargs='*')
    parser.add_argument('file',
                       help="file to upload",
                       nargs='*')

    args = parser.parse_args()

    if not args.directory and not args.file:
        parser.error("At least one directory or file is required")
    if not args.server:
        parser.error("An upload URL is required")

    return args


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

    _LOGGER.info(f"Starting upload of {len(upload_files)} file(s)")

    if args.skip_station and args.station in args.skip_station:
        for file in upload_files:
            _LOGGER.info(f"Skipping upload of {file} due to ignored station")
            if args.completed:
                target_file = Path(args.completed) / file.name
                try:
                    move_file(str(file), str(target_file))
                except IOError:
                    _LOGGER.warning(f"Failed to move {file} to completed location {target_file}", exc_info=True)
            continue
        exit(0)
        return

    upload_key = key.public_key()

    async def run():
        for file in upload_files:
            signature, contents = await prepare_file(file, key, args.compression)

            upload_ok = False
            for url in args.server:
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
                    exit(1)
                except:
                    _LOGGER.warning(f"Upload to {repr(url)} failed", exc_info=True)
                    continue
                upload_ok = True

            contents.close()

            if not upload_ok:
                _LOGGER.error(f"Upload of {file} failed")
                exit(1)

            if args.completed:
                target_dir = completion_directory(args.completed, upload_key, args.station, args.type)
                target_file = Path(target_dir) / file.name
                try:
                    move_file(str(file), str(target_file))
                except IOError:
                    _LOGGER.warning(f"Failed to move {file} to completed location {target_file}", exc_info=True)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.close()


if __name__ == '__main__':
    main()
