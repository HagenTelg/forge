import typing
import asyncio


_background_tasks: typing.Set[asyncio.Task] = set()


def background_task(coro: typing.Awaitable) -> asyncio.Task:
    r = asyncio.get_event_loop().create_task(coro)
    _background_tasks.add(r)
    r.add_done_callback(lambda task: _background_tasks.discard(r))
    return r
