import typing
import asyncio
import psutil
import time


async def add_disk_space(telemetry: typing.Dict[str, typing.Any]) -> None:
    try:
        usage = psutil.disk_usage('/')
    except OSError:
        return

    telemetry['root_total_bytes'] = usage.total
    telemetry['root_used_percent'] = usage.percent


_disk_accumulator_read: typing.Optional[int] = None
_disk_accumulator_write: typing.Optional[int] = None
_disk_accumulator_time: typing.Optional[float] = None


async def add_disk_rate(telemetry: typing.Dict[str, typing.Any]) -> None:
    global _disk_accumulator_read
    global _disk_accumulator_write
    global _disk_accumulator_time

    disk_counters = psutil.disk_io_counters()
    now = time.time()

    if _disk_accumulator_time is None:
        _disk_accumulator_read = disk_counters.read_bytes
        _disk_accumulator_write = disk_counters.write_bytes
        _disk_accumulator_time = now

        await asyncio.sleep(0.5)
        disk_counters = psutil.disk_io_counters()
        now = time.time()

    dT = now - _disk_accumulator_time
    if dT <= 0.0:
        return
    telemetry['disk_read'] = (disk_counters.read_bytes - _disk_accumulator_read) / dT
    telemetry['disk_write'] = (disk_counters.write_bytes - _disk_accumulator_write) / dT

    _disk_accumulator_read = disk_counters.read_bytes
    _disk_accumulator_write = disk_counters.write_bytes
    _disk_accumulator_time = now
