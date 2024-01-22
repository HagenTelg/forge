import typing
import time
import numpy as np
from math import floor, ceil
from json import dumps as to_json
from tempfile import NamedTemporaryFile
from netCDF4 import Dataset
from forge.logicaltime import containing_year_range, start_of_year
from forge.data.enum import remap_enum
from forge.data.structure import passed_data
from forge.data.structure.passed import passed_structure
from forge.archive.client import passed_lock_key, passed_file_name, passed_notification_key
from forge.archive.client.connection import Connection


def write_passed(
        input_file: typing.Optional[str], output_file: str, file_start: float, file_end: float,
        station: str, profile: str,
        start_epoch: float, end_epoch: float, time_of_pass: float,
        comment: str, auxiliary: typing.Optional[typing.Dict[str, typing.Any]],
) -> None:
    all_profiles: typing.Set[str] = {profile}
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
            for var in ("start_time", "end_time", "pass_time", ):
                output_root.variables[var][:] = input_root.variables[var][:]
            for var in ("comment", "auxiliary_data", ):
                input_var = input_root.variables[var]
                output_var = output_root.variables[var]
                for idx in range(input_var.shape[0]):
                    output_var[idx] = input_var[idx]
            for var in ("profile", ):
                remap_enum(input_root.variables[var], output_root.variables[var])

        output_idx = output_root.dimensions["index"].size
        for var, source in (
                ("start_time", int(floor(start_epoch * 1000))),
                ("end_time", int(ceil(end_epoch * 1000))),
                ("pass_time", int(ceil(time_of_pass * 1000))),
                ("profile", output_root.variables["profile"].datatype.enum_dict[profile]),
                ("comment", comment),
                ("auxiliary_data", to_json(auxiliary, sort_keys=True) if auxiliary else ""),
        ):
            output_root.variables[var][output_idx] = source
    finally:
        output_file.close()
        if input_file:
            input_file.close()


async def apply_pass(
        connection: Connection,
        station: str, profile: str, start_epoch: float, end_epoch: float,
        comment: str, auxiliary: typing.Optional[typing.Dict[str, typing.Any]],
) -> None:
    start_year, end_year = containing_year_range(start_epoch, end_epoch)
    await connection.lock_write(
        passed_lock_key(station),
        start_of_year(start_year),
        start_of_year(end_year),
    )

    pass_time = time.time()

    for year in range(start_year, end_year):
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

            write_passed(
                original_file.name if original_file else None, passed_file.name,
                year_start, year_end,
                station, profile,
                start_epoch, end_epoch, pass_time,
                comment, auxiliary
            )
            if original_file:
                original_file.close()

            passed_file.seek(0)
            await connection.write_file(archive_name, passed_file)

    await connection.send_notification(
        passed_notification_key(station),
        int(floor(start_epoch * 1000)),
        int(ceil(end_epoch * 1000)),
    )
