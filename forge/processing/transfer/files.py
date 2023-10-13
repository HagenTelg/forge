import typing
import asyncio
import logging
from pathlib import Path
from io import BytesIO
from tempfile import NamedTemporaryFile
from ftplib import FTP
from base64 import b64encode
from os import environ
from json import dumps as to_json
from forge.tasks import wait_cancelable
from forge.authsocket import PublicKey, key_to_bytes


_LOGGER = logging.getLogger(__name__)


def upload_ftp(ftp: FTP, file: Path, contents: typing.BinaryIO, public_key: PublicKey, signature: bytes) -> None:
    signature_file = to_json({
        'public_key': b64encode(key_to_bytes(public_key)).decode('ascii'),
        'signature': b64encode(signature).decode('ascii'),
    }).encode('utf-8')
    ftp.storbinary(f'STOR {file.name}.sig', BytesIO(signature_file))
    ftp.storbinary(f'STOR {file.name}', contents)


def _upload_sftp_paramiko(
        path: str, file: Path, contents: typing.BinaryIO, public_key: PublicKey, signature: bytes,
        sftp
) -> None:
    if path:
        try:
            sftp.chdir(path)
        except IOError:
            if path.startswith('/'):
                path = path[1:]
                sftp.chdir('/')
            for p in path.split('/'):
                if not p:
                    continue
                sftp.mkdir(p, mode=0o755)
                sftp.chdir(p)
    signature_file = to_json({
        'public_key': b64encode(key_to_bytes(public_key)).decode('ascii'),
        'signature': b64encode(signature).decode('ascii'),
    }).encode('utf-8')
    with sftp.open(f'{file.name}.sig', 'w') as f:
        f.write(signature_file)
    sftp.putfo(contents, file.name, confirm=True)


async def _upload_sftp_command(
        path: str, file: Path, contents: typing.BinaryIO, public_key: PublicKey, signature: bytes,
        hostname: str, port: int,
        username: typing.Optional[str], password: typing.Optional[str], key_filename: typing.Optional[str],
) -> None:
    sftp_command = [
        'sftp',
        '-o', 'UserKnownHostsFile=/dev/null',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'CheckHostIP=no',
        '-o', 'PasswordAuthentication=no',
        '-o', 'UpdateHostKeys=no',
        *(['-o', f'IdentityFile={key_filename}'] if key_filename else []),
        '-P', str(port),
        '-b', '-',
        f'{(username + "@") if username else ""}{hostname}'
    ]

    if password:
        environ['SSHPASS'] = password
        sftp_command = ['sshpass', '-e'] + sftp_command

    sftp = await asyncio.create_subprocess_exec(
        *sftp_command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.DEVNULL,
    )
    if path:
        if path.startswith('/'):
            path = path[1:]
            sftp.stdin.write(f'cd /\n'.encode('utf-8'))
        for p in path.split('/'):
            if not p:
                continue
            sftp.stdin.write(f'-mkdir "{p}"\n'.encode('utf-8'))
            sftp.stdin.write(f'cd "{p}"\n'.encode('utf-8'))

    with NamedTemporaryFile('wb') as signature_upload, NamedTemporaryFile('wb') as data_upload:
        signature_upload.write(to_json({
            'public_key': b64encode(key_to_bytes(public_key)).decode('ascii'),
            'signature': b64encode(signature).decode('ascii'),
        }).encode('utf-8'))
        while True:
            chunk = contents.read(65536)
            if not chunk:
                break
            data_upload.write(chunk)
        signature_upload.flush()
        data_upload.flush()
        sftp.stdin.write(f'put "{signature_upload.name}" "{file.name}.sig"\n'.encode('utf-8'))
        sftp.stdin.write(f'put "{data_upload.name}" "{file.name}"\n'.encode('utf-8'))
        sftp.stdin.write(f'quit\n'.encode('utf-8'))
        await sftp.stdin.drain()
        sftp.stdin.close()

        try:
            await wait_cancelable(sftp.wait(), 180.0)
        except:
            try:
                sftp.terminate()
            except:
                pass
            try:
                await wait_cancelable(sftp.wait(), 5.0)
            except (TimeoutError, asyncio.TimeoutError):
                try:
                    sftp.kill()
                except:
                    pass
                try:
                    await sftp.wait()
                except:
                    pass
            except:
                pass
            raise

    if sftp.returncode != 0:
        raise ValueError(f"SFTP upload failed with return code {sftp.returncode}")


async def upload_sftp(
        path: str, file: Path, contents: typing.BinaryIO, public_key: PublicKey, signature: bytes,
        hostname: str, port: int,
        username: typing.Optional[str], password: typing.Optional[str],
        key_file: typing.Optional[str], key_passphrase: typing.Optional[str],
) -> None:
    try:
        from paramiko.client import SSHClient, WarningPolicy
        with SSHClient() as ssh:
            ssh.set_missing_host_key_policy(WarningPolicy)
            ssh.connect(hostname, port,
                        username=username, password=password,
                        key_filename=key_file, passphrase=key_passphrase,
                        timeout=180)
            with ssh.open_sftp() as sftp:
                _upload_sftp_paramiko(path, file, contents, public_key, signature, sftp)
        return
    except ImportError:
        pass
    _LOGGER.info("Paramiko not available, using command fallback")
    await _upload_sftp_command(path, file, contents, public_key, signature,
                               hostname, port, username=username, password=password,
                               key_filename=key_file)
