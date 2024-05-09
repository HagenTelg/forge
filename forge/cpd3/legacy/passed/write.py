import typing
import asyncio
import logging
import numpy as np
from math import floor, ceil
from netCDF4 import Dataset
from tempfile import NamedTemporaryFile
from forge.cpd3.identity import Identity
from forge.cpd3.variant import to_json
from forge.logicaltime import start_of_year
from forge.logicaltime import containing_year_range
from forge.archive.client import passed_file_name, passed_lock_key, passed_notification_key
from forge.archive.client.connection import Connection, LockBackoff, LockDenied
from forge.data.structure import passed_data
from forge.data.structure.passed import passed_structure
from forge.data.enum import remap_enum

_LOGGER = logging.getLogger(__name__)


class PassedTime:
    def __init__(self, identity: Identity, info: typing.Dict[str, typing.Any],
                 modified: typing.Optional[float] = None):
        self.start_epoch: float = identity.start
        assert self.start_epoch
        self.end_epoch: float = identity.end
        assert self.end_epoch
        assert self.start_epoch < self.end_epoch

        self.comment: str = str(info.get("Comment", ""))

        info = info.get("Information", {})

        self.profile: str = identity.variable
        self.pass_time: int = int(round((modified if modified else float(info.get("At", self.end_epoch))) * 1000))
        self.auxiliary_data: str = to_json({
            "type": "cpd3",
            "environment": info.get("Environment", ""),
            "revision": info.get("Revision", ""),
        }, sort_keys=True)

    @property
    def affected_years(self) -> typing.Tuple[int, int]:
        return containing_year_range(self.start_epoch, self.end_epoch)

    @property
    def start_time(self) -> int:
        return int(floor(self.start_epoch * 1000))

    @property
    def end_time(self) -> int:
        return int(ceil(self.end_epoch * 1000))


def write_all(
        station: str,
        year_data: typing.Dict[int, typing.List[PassedTime]]
) -> typing.Tuple[int, int]:
    total = 0
    modified = 0

    def modify_passed(
            input_file: typing.Optional[str], output_file: str,  file_start: float, file_end: float,
            merge_info: typing.List[PassedTime],
    ) -> typing.Set[typing.Tuple[int, int]]:
        all_profiles: typing.Set[str] = set()
        for info in merge_info:
            all_profiles.add(info.profile)

        input_root = None
        if input_file:
            input_file = Dataset(input_file, 'r')
            input_root = input_file.groups.get("passed")
            if input_root is not None:
                all_profiles.update(input_root.variables["profile"].datatype.enum_dict.keys())
        else:
            input_file = None

        output_file = Dataset(output_file, 'w', format='NETCDF4')
        try:
            passed_data(output_file, station, file_start, file_end)
            passed_structure(output_file, sorted(all_profiles))
            output_root = output_file.groups["passed"]

            if input_root is not None:
                for var in ("start_time", "end_time", "pass_time",):
                    output_root.variables[var][:] = input_root.variables[var][:]
                for var in ("comment", "auxiliary_data",):
                    input_var = input_root.variables[var]
                    output_var = output_root.variables[var]
                    for idx in range(input_var.shape[0]):
                        output_var[idx] = input_var[idx]
                for var in ("profile",):
                    remap_enum(input_root.variables[var], output_root.variables[var])

            modified_ranges: typing.Set[typing.Tuple[int, int]] = set()
            for info in merge_info:
                if np.any(np.all((
                    output_root.variables['start_time'][:].data == info.start_time,
                    output_root.variables['end_time'][:].data == info.end_time,
                    output_root.variables['pass_time'][:].data == info.pass_time,
                    output_root.variables['profile'][:].data == output_root.variables["profile"].datatype.enum_dict.get(info.profile, -1),
                ), axis=0)):
                    continue
                output_idx = output_root.dimensions["index"].size
                for var, source in (
                        ("start_time", info.start_time),
                        ("end_time", info.end_time),
                        ("pass_time", info.pass_time),
                        ("profile", output_root.variables["profile"].datatype.enum_dict[info.profile]),
                        ("comment", info.comment),
                        ("auxiliary_data", info.auxiliary_data),
                ):
                    output_root.variables[var][output_idx] = source
                modified_ranges.add((info.start_time, info.end_time))

            return modified_ranges
        finally:
            output_file.close()
            if input_file:
                input_file.close()

    async def run() -> None:
        nonlocal total
        nonlocal modified

        async with (await Connection.default_connection("write legacy passed")) as connection:
            min_year = min(year_data.keys())
            max_year = max(year_data.keys())

            backoff = LockBackoff()
            while True:
                total = 0
                modified = 0
                async with connection.transaction(True):
                    try:
                        await connection.lock_write(
                            passed_lock_key(station),
                            start_of_year(min_year), start_of_year(max_year+1),
                        )

                        for year, merge_info in year_data.items():
                            year_start = start_of_year(year)
                            year_end = start_of_year(year + 1)
                            archive_name = passed_file_name(station, year_start)

                            with NamedTemporaryFile(suffix=".nc") as original_file, NamedTemporaryFile(suffix=".nc") as passed_file:
                                try:
                                    await connection.read_file(archive_name, original_file)
                                    original_file.flush()
                                except FileNotFoundError:
                                    original_file.close()
                                    original_file = None

                                modified_ranges = modify_passed(
                                    original_file.name if original_file else None, passed_file.name,
                                    year_start, year_end,
                                    merge_info,
                                )
                                if original_file:
                                    original_file.close()

                                if modified_ranges:
                                    passed_file.seek(0)
                                    _LOGGER.debug(f"Writing passed data for {station.upper()}/{year}")
                                    await connection.write_file(archive_name, passed_file)
                                    for start, end in modified_ranges:
                                        await connection.send_notification(passed_notification_key(station), start, end)

                            total += len(merge_info)
                            modified += len(modified_ranges)

                        break
                    except LockDenied as ld:
                        _LOGGER.info("Archive busy: %s", ld.status)
                        await backoff()
                        continue

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.close()
    return total, modified
