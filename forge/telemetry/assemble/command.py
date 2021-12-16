import typing
import asyncio


async def command_output(*command, silent=False, check_exit=False) -> typing.Optional[str]:
    stderr = None
    if silent:
        stderr = asyncio.subprocess.DEVNULL
    process = await asyncio.create_subprocess_exec(*command,
                                                   stdout=asyncio.subprocess.PIPE,
                                                   stdin=asyncio.subprocess.DEVNULL,
                                                   stderr=stderr)
    raw = await process.stdout.read()
    await process.wait()

    if check_exit:
        if process.returncode != 0:
            return None

    return raw.decode('utf-8').strip()


async def command_lines(*command, silent=False, check_exit=False) -> typing.List[str]:
    stderr = None
    if silent:
        stderr = asyncio.subprocess.DEVNULL
    process = await asyncio.create_subprocess_exec(*command,
                                                   stdout=asyncio.subprocess.PIPE,
                                                   stdin=asyncio.subprocess.DEVNULL,
                                                   stderr=stderr)
    result: typing.List[str] = list()
    while True:
        line = await process.stdout.readline()
        if not line:
            break
        line = line.decode('utf-8').strip()
        if not line:
            continue
        result.append(line)

    await process.wait()

    if check_exit:
        if process.returncode != 0:
            return []

    return result
