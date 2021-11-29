import typing
import asyncio
from json import loads as from_json


_ARGS_KERNEL = ['--dmesg', '--boot']
_ARGS_SYSTEM = ['--system']
_ARGS_ACQUISITION = ['--unit=cpd3-acquisition.service']


def _convert_json_line(line: bytes) -> typing.Dict[str, typing.Any]:
    event = from_json(line)
    timestamp = (event.get('_SOURCE_REALTIME_TIMESTAMP') or event.get('__SOURCE_REALTIME_TIMESTAMP') or
                 event.get('_REALTIME_TIMESTAMP') or event.get('__REALTIME_TIMESTAMP'))
    return {
        'message': event.get('MESSAGE'),
        'time': timestamp and int(timestamp) / 1E6,
        'source': event.get('_SYSTEMD_UNIT') or event.get('_COMM'),
    }


async def _read_journalctl(*args) -> typing.List[typing.Dict[str, typing.Any]]:
    journalctl = await asyncio.create_subprocess_exec('journalctl', '--utc', '--quiet', '--output=json',
                                                      '--lines=100',
                                                      *args,
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=asyncio.subprocess.DEVNULL)

    result: typing.List[typing.Dict[str, typing.Any]] = list()
    while True:
        line = await journalctl.stdout.readline()
        if not line:
            break
        result.append(_convert_json_line(line))

    await journalctl.wait()
    return result


async def add_kernel_log(telemetry: typing.Dict[str, typing.Any]) -> None:
    telemetry['log_kernel'] = await _read_journalctl(*_ARGS_KERNEL)


async def add_system_log(telemetry: typing.Dict[str, typing.Any]) -> None:
    telemetry['log_system'] = await _read_journalctl(*_ARGS_SYSTEM)


async def add_acquisition_log(telemetry: typing.Dict[str, typing.Any]) -> None:
    telemetry['log_acquisition'] = await _read_journalctl(*_ARGS_ACQUISITION)


async def _stream_journalctl(*args) -> typing.Tuple[asyncio.Task, typing.AsyncGenerator]:
    journalctl = await asyncio.create_subprocess_exec('journalctl', '--utc', '--quiet', '--output=json',
                                                      '--lines=0', '--follow',
                                                      *args,
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stdin=asyncio.subprocess.DEVNULL)

    async def _wait_process():
        try:
            await journalctl.wait()
        except asyncio.CancelledError:
            try:
                journalctl.terminate()
                await journalctl.wait()
            except OSError:
                pass
            raise

    task = asyncio.ensure_future(_wait_process())

    async def _read_events():
        while True:
            line = await journalctl.stdout.readline()
            if not line:
                break
            yield _convert_json_line(line)

    return task, _read_events()


async def stream_kernel_log() -> typing.Tuple[asyncio.Task, typing.AsyncGenerator]:
    return await _stream_journalctl(*_ARGS_KERNEL)


async def stream_system_log() -> typing.Tuple[asyncio.Task, typing.AsyncGenerator]:
    return await _stream_journalctl(*_ARGS_SYSTEM)


async def stream_acquisition_log() -> typing.Tuple[asyncio.Task, typing.AsyncGenerator]:
    return await _stream_journalctl(*_ARGS_ACQUISITION)
