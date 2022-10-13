import typing
import asyncio

_background_tasks: typing.Set[asyncio.Task] = set()


def background_task(coro: typing.Awaitable) -> asyncio.Task:
    r = asyncio.get_event_loop().create_task(coro)
    _background_tasks.add(r)
    r.add_done_callback(lambda task: _background_tasks.discard(r))
    return r


async def _cancel_timed_out(fut):
    fut.cancel()
    try:
        await fut
    except asyncio.CancelledError:
        pass


async def wait_cancelable(fut, timeout):
    # Unfortunately, asyncio.wait_for consumes cancellation when the inner has finished but not yet been awaited
    # at the moment of cancellation (python 42130).  So this is a re-implementation that prioritizes cancellation.

    fut = asyncio.ensure_future(fut)

    try:
        done, pending = await asyncio.wait({fut}, timeout=timeout, return_when=asyncio.FIRST_EXCEPTION)
    except asyncio.CancelledError:
        await asyncio.shield(_cancel_timed_out(fut))
        raise

    if pending:
        if fut.done():
            e = fut.exception()
            if e is not None:
                # Done via exception (possibly cancellation), so raise that
                raise e
            # Otherwise, consider it a timeout
            raise asyncio.TimeoutError()

        # Now it's a timeout, unless cancellation happens during the wait (raised since it's not caught)
        await asyncio.shield(_cancel_timed_out(fut))
        raise asyncio.TimeoutError()

    return fut.result()
