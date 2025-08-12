import typing
import asyncio
import logging
import re
import numpy as np
from pathlib import Path
from tempfile import NamedTemporaryFile
from netCDF4 import Dataset
from forge.const import MAX_I64, STATIONS
from forge.archive.client import passed_lock_key
from forge.archive.client.connection import Connection, LockDenied, LockBackoff
from forge.vis.data.stream import DataStream, ArchiveReadStream

_LOGGER = logging.getLogger(__name__)


_PASSED_FILE_MATCH = re.compile(r'[A-Z][0-9A-Z_]{0,31}-PASSED_s(\d{4})0101\.nc')


async def _walk_passed_files(connection: Connection, station: str,
                             walk_backwards: bool = False) -> typing.AsyncIterable[Dataset]:
    passed_files: typing.List[typing.Tuple[int, str]] = []

    for file in await connection.list_files(f"passed/{station}/"):
        file = Path(file)
        match = _PASSED_FILE_MATCH.match(file.name)
        if not match:
            continue
        year = int(match.group(1))
        passed_files.append((year, str(file)))

    passed_files.sort(key=lambda x: x[0])
    if walk_backwards:
        passed_files.reverse()

    _LOGGER.debug(f"Reading {len(passed_files)} passed files for {station.upper()}")
    for _, file in passed_files:
        with NamedTemporaryFile(suffix=".nc") as data_file:
            try:
                await connection.read_file(file, data_file)
                data_file.flush()
            except FileNotFoundError:
                continue
            data = Dataset(data_file.name, 'r')
            try:
                yield data
            finally:
                data.close()


async def _get_passed_files(station: str, walk_backwards: bool = False) -> typing.AsyncIterable[Dataset]:
    async with await Connection.default_connection("read passed", use_environ=False) as connection:
        backoff = LockBackoff()
        while True:
            try:
                async with connection.transaction():
                    await connection.lock_read(passed_lock_key(station), -MAX_I64, MAX_I64)
                    async for data in _walk_passed_files(connection, station, walk_backwards):
                        yield data
                    break
            except LockDenied:
                await backoff()
                continue


async def read_latest_passed(station: str, mode_name: str) -> typing.Optional[int]:
    station = station.lower()
    if station not in STATIONS:
        raise ValueError(f"Invalid station {station}")
    profile = mode_name.split('-', 1)[0].lower()
    if not profile:
        raise ValueError(f"Invalid profile {profile}")

    async for file in _get_passed_files(station, True):
        passed_data = file.groups.get("passed")
        if passed_data is None:
            continue
        pass_profile = passed_data.variables["profile"]
        pass_profile_value = pass_profile.datatype.enum_dict.get(profile)
        if pass_profile_value is None:
            continue
        applicable_passes = pass_profile[...].data == pass_profile_value
        if not np.any(applicable_passes):
            continue

        pass_end = passed_data.variables["end_time"][...].data
        pass_end = pass_end[applicable_passes]
        last_pass_end = int(np.max(pass_end))
        _LOGGER.debug(f"Latest passed for {station.upper()}/{profile.upper()} found at {last_pass_end}")
        return last_pass_end

    return None


class _PassedReadStream(ArchiveReadStream):
    def __init__(self, station: str, profile: str, send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)
        self.station = station.lower()
        self.profile = profile.lower()

    @property
    def connection_name(self) -> str:
        return "read passed"

    async def acquire_locks(self) -> None:
        await self.connection.lock_read(passed_lock_key(self.station), -MAX_I64, MAX_I64)

    async def with_locks_held(self) -> None:
        async for file in _walk_passed_files(self.connection, self.station):
            passed_data = file.groups.get("passed")
            if passed_data is None:
                continue
            pass_profile = passed_data.variables["profile"]
            pass_profile_value = pass_profile.datatype.enum_dict.get(self.profile)
            if pass_profile_value is None:
                continue
            applicable_passes = pass_profile[...].data == pass_profile_value
            if not np.any(applicable_passes):
                continue

            pass_start = passed_data.variables["start_time"][...].data
            pass_start = pass_start[applicable_passes]
            pass_end = passed_data.variables["end_time"][...].data
            pass_end = pass_end[applicable_passes]
            pass_time = passed_data.variables["pass_time"][...].data
            pass_time = pass_time[applicable_passes]
            pass_comment = passed_data.variables["comment"][...]  # String type does not give a masked array
            pass_comment = pass_comment[applicable_passes]

            for idx in np.ndindex(pass_start.shape):
                await self.send({
                    'start_epoch_ms': int(pass_start[idx]),
                    'end_epoch_ms': int(pass_end[idx]),
                    'pass_time_epoch_ms': int(pass_time[idx]),
                    'comment': str(pass_comment[idx]),
                })


def read_passed(station: str, mode_name: str,
                      send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    station = station.lower()
    if station not in STATIONS:
        raise ValueError(f"Invalid station {station}")
    profile = mode_name.split('-', 1)[0].lower()
    if not profile:
        raise ValueError(f"Invalid profile {profile}")
    return _PassedReadStream(station, profile, send)
