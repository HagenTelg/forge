import typing
import logging
import asyncio
import argparse
import sys
import numpy as np
from pathlib import Path
from math import floor, ceil
from netCDF4 import Dataset, Variable
from forge.const import STATIONS
from forge.timeparse import parse_time_bounds_arguments
from forge.formattime import format_iso8601_time
from forge.logicaltime import containing_year_range, start_of_year
from forge.temp import WorkingDirectory
from forge.archive.client import edit_directives_lock_key, edit_directives_file_name
from forge.archive.client.get import read_file_or_nothing
from forge.archive.client.connection import Connection, LockDenied, LockBackoff
from forge.data.structure import edit_directives
from forge.data.structure.editdirectives import edit_file_structure

_LOGGER = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Read raw format edit directives")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--output',
                        dest='output',
                        help="output NetCDF4 file name")
    parser.add_argument('station', help="station code to fetch")
    parser.add_argument('time', help="time bounds to retrieve all intersecting edits for", nargs='+')

    args = parser.parse_args()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    station = args.station.lower()
    if station not in STATIONS:
        parser.error("Invalid station code")
    start, end = parse_time_bounds_arguments(args.time)
    start = start.timestamp()
    end = end.timestamp()
    start_ms = int(floor(start * 1000))
    end_ms = int(ceil(end * 1000))

    loop = asyncio.new_event_loop()

    output_file: typing.Optional[Dataset] = None
    output_count: int = 0

    def matching_edits(directives_root: Dataset) -> np.ndarray:
        edit_start_time: Variable = directives_root.variables["start_time"]
        edit_end_time: Variable = directives_root.variables["end_time"]
        edit_start_time: np.ndarray = edit_start_time[...].data
        edit_end_time: np.ndarray = edit_end_time[...].data

        active_edits = np.all((
            edit_start_time < end_ms,
            edit_end_time > start_ms,
        ), axis=0)
        return np.where(active_edits)[0]

    async def run():
        nonlocal output_count

        connection = await Connection.default_connection("read edit directives")
        await connection.startup()

        async with WorkingDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            backoff = LockBackoff()
            try:
                while True:
                    try:
                        async with connection.transaction():
                            await connection.lock_read(edit_directives_lock_key(station), start_ms, end_ms)
                            await read_file_or_nothing(connection, edit_directives_file_name(station, None), tmpdir)
                            for year in range(*containing_year_range(start, end)):
                                year_start = start_of_year(year)
                                await read_file_or_nothing(connection, edit_directives_file_name(station, year_start), tmpdir)
                    except LockDenied as ld:
                        _LOGGER.debug("Archive busy: %s", ld.status)
                        if sys.stdout.isatty():
                            if not backoff.has_failed:
                                sys.stdout.write("\n")
                            sys.stdout.write(f"\x1B[2K\rBusy: {ld.status}")
                        await backoff()
                        continue
                    break
            finally:
                if backoff.has_failed and sys.stdout.isatty():
                    sys.stdout.write("\n")

            def get_output() -> Dataset:
                nonlocal output_file
                if output_file is not None:
                    return output_file.groups["edits"]

                possible_profiles: typing.Set[str] = set()
                for input_file in tmpdir.iterdir():
                    if not input_file.is_file():
                        continue
                    input_file = Dataset(str(input_file), 'r')
                    try:
                        directives_root = input_file.groups["edits"]
                        profiles = directives_root.variables["profile"]
                        profile_lookup: typing.Dict[int, str] = dict()
                        for code, id_number in profiles.datatype.enum_dict.items():
                            profile_lookup[id_number] = code
                        profiles = profiles[...].data
                        for idx in matching_edits(directives_root):
                            possible_profiles.add(profile_lookup[profiles[idx]])
                    finally:
                        input_file.close()

                if args.output is not None:
                    destination_name = args.output
                else:
                    destination_name = f"{station.upper()}-EDITS.nc"
                output_file = Dataset(destination_name, 'w', format='NETCDF4')
                edit_directives(output_file, station, start, end)
                edits = edit_file_structure(output_file, sorted(possible_profiles))
                return edits

            for input_file in tmpdir.iterdir():
                if not input_file.is_file():
                    continue
                input_file = Dataset(str(input_file), 'r')
                try:
                    input_root = input_file.groups["edits"]

                    input_profile = input_root.variables["profile"]
                    input_profile_lookup: typing.Dict[int, str] = dict()
                    for code, id_number in input_profile.datatype.enum_dict.items():
                        input_profile_lookup[id_number] = code

                    input_action = input_root.variables["action_type"]
                    input_action_lookup: typing.Dict[int, str] = dict()
                    for code, id_number in input_action.datatype.enum_dict.items():
                        input_action_lookup[id_number] = code

                    input_condition = input_root.variables["condition_type"]
                    input_condition_lookup: typing.Dict[int, str] = dict()
                    for code, id_number in input_condition.datatype.enum_dict.items():
                        input_condition_lookup[id_number] = code

                    for input_idx in matching_edits(input_root):
                        output_count += 1
                        output_root = get_output()
                        output_idx = output_root.dimensions["index"].size

                        for var in ("start_time", "end_time", "modified_time", "unique_id", "deleted",
                                    "action_parameters", "condition_parameters", "author", "comment", "history"):
                            output_root.variables[var][output_idx] = input_root.variables[var][input_idx]

                        for var, input_map in (
                                ("profile", input_profile_lookup),
                                ("action_type", input_action_lookup),
                                ("condition_type", input_condition_lookup),
                        ):
                            value = input_root.variables[var][input_idx]
                            if value is None or value.mask:
                                continue
                            input_code = input_map[int(value)]
                            output_var = output_root.variables[var]
                            output_map = output_var.datatype.enum_dict
                            output_root.variables[var][output_idx] = output_map[input_code]
                finally:
                    input_file.close()

        await connection.shutdown()

    loop.run_until_complete(run())
    loop.close()

    if output_file is None:
        print(f"No edits found for {station.upper()} within {format_iso8601_time(start)} to {format_iso8601_time(end)}")
    else:
        print(f"Wrote {output_count} edit(s) to '{output_file.filepath()}'")


if __name__ == '__main__':
    main()
