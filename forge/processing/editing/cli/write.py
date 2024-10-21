import typing
import logging
import asyncio
import argparse
import time
import random
import re
import os
import sys
import numpy as np
from math import ceil
from tempfile import TemporaryDirectory, mkstemp
from pathlib import Path
from netCDF4 import Dataset, Variable
from forge.logicaltime import containing_year_range, start_of_year, year_bounds
from forge.const import STATIONS, MAX_I64
from forge.archive.client import edit_directives_lock_key, edit_directives_file_name, edit_directives_notification_key
from forge.archive.client.connection import Connection, LockDenied, LockBackoff
from forge.data.enum import remap_enum
from forge.data.structure import edit_directives
from forge.data.structure.editdirectives import edit_file_structure

_LOGGER = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Create or modify raw format edit directives")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--use-modified-time',
                        dest='use_modified_time', action='store_true',
                        help="use the modification time from the input file instead of the current time")
    parser.add_argument('file', help="file containing edit directives")

    args = parser.parse_args()
    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    input_file = Dataset(args.file, 'r')

    station = input_file.variables.get("station_name")
    if station is None:
        parser.error("Invalid input file")
    station = str(station[0]).lower()
    if station not in STATIONS:
        parser.error("Invalid station")
    input_root = input_file.groups.get("edits")
    if input_root is None:
        parser.error("Invalid input file")
    input_start_time: Variable = input_root.variables.get("start_time")
    input_end_time: Variable = input_root.variables.get("end_time")
    if input_start_time is None or input_end_time is None:
        parser.error("Invalid input file")
    input_start_time: np.ndarray = input_start_time[...].data
    if len(input_start_time.shape) == 0 or input_start_time.shape[0] == 0:
        print(f"No edits found in file '{args.file}'")
        input_file.close()
        return
    input_end_time: np.ndarray = input_end_time[...].data
    input_uid: Variable = input_root.variables.get("unique_id")
    if input_uid is None:
        input_uid: np.ndarray = np.full(input_start_time.shape, 0, dtype=np.uint64)
    else:
        input_uid: np.ndarray = np.ma.filled(input_uid[...], fill_value=0)

    if args.use_modified_time:
        input_modified: Variable = input_root.variables.get("modified_time")
        if input_modified is None:
            parser.error("No modification time present in the input file")
        modification_time = input_modified[...].data
    else:
        modification_time = np.full(input_start_time.shape, int(ceil(time.time() * 1000)), dtype=np.int64)

    input_profile: Variable = input_root.variables["profile"]
    input_profile_values = input_profile.datatype.enum_dict
    input_profile_lookup: typing.Dict[int, str] = dict()
    for code, id_number in input_profile_values.items():
        input_profile_lookup[id_number] = code

    input_action: Variable = input_root.variables["action_type"]
    input_action_lookup: typing.Dict[int, str] = dict()
    for code, id_number in input_action.datatype.enum_dict.items():
        input_action_lookup[id_number] = code

    input_condition: Variable = input_root.variables["condition_type"]
    input_condition_lookup: typing.Dict[int, str] = dict()
    for code, id_number in input_condition.datatype.enum_dict.items():
        input_condition_lookup[id_number] = code

    loop = asyncio.new_event_loop()

    edit_file_match = re.compile(
        station.upper() + r'-EDITS_'
        r's(\d{4})\d{2}\d{2}\.nc',
    )

    year_files: typing.Dict[int, typing.Tuple[Path, typing.Optional[Dataset], bool]] = dict()
    modified_edit_count: int = 0

    async def run():
        nonlocal modified_edit_count

        connection = await Connection.default_connection("write edit directives")
        await connection.startup()

        with TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            allocated_uids: typing.Set[int] = set(input_uid)

            def new_uid() -> int:
                while True:
                    uid = random.randint(1, 1 << 64)
                    if uid in allocated_uids:
                        continue
                    allocated_uids.add(uid)
                    return uid

            def remove_uid(year: int, uid: int, input_idx: int) -> typing.Optional[typing.Tuple[int, int]]:
                existing = year_files.get(year)
                if existing is None:
                    return None
                existing_path, existing_file, _ = existing
                if existing_file is None:
                    return None
                existing_root = existing_file.groups["edits"]
                existing_uid = existing_root.variables["unique_id"][...].data
                if uid == 0:
                    while True:
                        hit = np.where(existing_uid == input_uid[input_idx])[0]
                        if hit.shape[0] == 0:
                            break
                        input_uid[input_idx] = new_uid()
                    return None
                hit = np.where(existing_uid == uid)[0]
                if hit.shape[0] == 0:
                    return None
                assert hit.shape[0] == 1
                remove_index = int(hit[0])
                exiting_start_time = int(existing_root.variables["start_time"][...].data[remove_index])
                exiting_end_time = int(existing_root.variables["end_time"][...].data[remove_index])
                if existing_uid.shape[0] == 1:
                    existing_file.close()
                    existing_path.unlink()
                    year_files[year] = (Path(), None, True)
                    return exiting_start_time, exiting_end_time

                fd, modified_name = mkstemp(suffix='.nc', dir=tmpdir)
                os.close(fd)
                output_file = Dataset(modified_name, 'w', format='NETCDF4')
                year_files[year] = (Path(modified_name), output_file, True)
                if year != 0:
                    file_start, file_end = year_bounds(year)
                else:
                    file_start = None
                    file_end = None
                edit_directives(output_file, station, file_start, file_end)
                output_root = edit_file_structure(
                    output_file,
                    existing_root.variables["profile"].datatype.enum_dict
                )

                for var in ("start_time", "end_time", "modified_time", "unique_id"):
                    input_var = input_root.variables[var]
                    output_var = output_root.variables[var]
                    if remove_index != 0:
                        output_var[:remove_index] = input_var[:remove_index]
                    if remove_index != input_var.shape[0] - 1:
                        output_var[remove_index:] = input_var[remove_index+1:]

                for var in ("profile", "action_type", "condition_type", "deleted"):
                    remap_enum(input_root.variables[var], output_root.variables[var])

                for var in ("action_parameters", "condition_parameters", "author", "comment", "history"):
                    input_var = input_root.variables[var]
                    output_var = output_root.variables[var]
                    for input_idx in range(input_var.shape[0]):
                        if input_idx == remove_index:
                            continue
                        output_var[output_var.shape[0]] = input_var[input_idx]

                existing_file.close()
                return exiting_start_time, exiting_end_time

            def make_unique_uid(year: int, input_idx: int):
                existing = year_files.get(year)
                if existing is None:
                    return
                _, existing_file, _ = existing
                if existing_file is None:
                    return
                output_uid = existing_file.variables["unique_id"][...].data
                while True:
                    hit = np.where(output_uid == input_uid[input_idx])[0]
                    if hit.shape[0] == 0:
                        break
                    input_uid[input_idx] = new_uid()

            def modify_edit(year: int, uid: int, input_idx: int) -> typing.Tuple[int, int]:
                def acquire_edits() -> Dataset:
                    existing = year_files.get(year)
                    copy_root = None
                    existing_file = None
                    if existing is not None and existing[1] is not None:
                        existing_path, existing_file, modified = existing
                        existing_root = existing_file.groups["edits"]

                        existing_profiles = existing_root.variables["profile"].datatype.enum_dict
                        for check_profile in input_profile_values.keys():
                            if check_profile not in existing_profiles:
                                break
                        else:
                            if not modified:
                                year_files[year] = (existing_path, existing_file, True)
                            return existing_root

                        copy_root = existing_root

                    fd, modified_name = mkstemp(suffix='.nc', dir=tmpdir)
                    os.close(fd)
                    output_file = Dataset(modified_name, 'w', format='NETCDF4')
                    year_files[year] = (Path(modified_name), output_file, True)
                    if year != 0:
                        file_start, file_end = year_bounds(year)
                    else:
                        file_start = None
                        file_end = None
                    edit_directives(output_file, station, file_start, file_end)

                    if copy_root is not None:
                        all_profiles = set(copy_root.variables["profile"].datatype.enum_dict.keys())
                        all_profiles.update(input_profile_values.keys())

                        output_root = edit_file_structure(output_file, sorted(all_profiles))

                        for var in ("start_time", "end_time", "modified_time", "unique_id"):
                            input_var = copy_root.variables[var]
                            output_var = output_root.variables[var]
                            input_var[:] = output_var[:]
                        for var in ("action_parameters", "condition_parameters", "author", "comment", "history"):
                            input_var = copy_root.variables[var]
                            output_var = output_root.variables[var]
                            for input_idx in range(input_var.shape[0]):
                                output_var[input_idx] = input_var[input_idx]
                        for var in ("profile", "action_type", "condition_type", "deleted"):
                            remap_enum(copy_root.variables[var], output_root.variables[var])
                    else:
                        output_root = edit_file_structure(output_file, input_profile_values)

                    if existing_file is not None:
                        existing_file.close()

                    return output_root

                existing_start = int(input_start_time[input_idx])
                existing_end = int(input_end_time[input_idx])

                output_root = acquire_edits()
                output_uid = output_root.variables["unique_id"]
                if uid != 0:
                    replace_idx = np.where(output_uid[...].data == uid)[0]
                    if replace_idx.shape[0] == 0:
                        output_idx = int(output_uid.shape[0])
                    else:
                        assert replace_idx.shape[0] == 1
                        output_idx = int(replace_idx[0])
                        existing_start = output_root.variables["start_time"][output_idx]
                        existing_end = output_root.variables["end_time"][output_idx]
                else:
                    output_idx = int(output_uid.shape[0])

                output_root.variables["unique_id"][output_idx] = input_uid[input_idx]
                output_root.variables["modified_time"][output_idx] = modification_time[input_idx]
                for var in ("start_time", "end_time", "deleted",
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

                return existing_start, existing_end

            backoff = LockBackoff()
            while True:
                modified_edit_count = 0
                try:
                    async with connection.transaction(True):
                        await connection.lock_write(edit_directives_lock_key(station), -MAX_I64, MAX_I64)

                        await connection.set_transaction_status("Loading edit directives for modification")
                        for archive_path in await connection.list_files(f"edits/{station.lower()}"):
                            output_file = tmpdir / Path(archive_path).name
                            with output_file.open("wb") as f:
                                try:
                                    await connection.read_file(archive_path, f)
                                except FileNotFoundError:
                                    output_file.unlink()
                                    continue
                            match = edit_file_match.fullmatch(Path(archive_path).name)
                            if not match:
                                destination_year = 0
                            else:
                                destination_year = int(match.group(1))

                            edit_file = Dataset(str(output_file), 'r+')
                            year_files[destination_year] = (output_file, edit_file, False)

                        for input_idx in range(input_start_time.shape[0]):
                            edit_start = int(input_start_time[input_idx])
                            edit_end = int(input_end_time[input_idx])
                            edit_uid = int(input_uid[input_idx])
                            if edit_uid == 0:
                                input_uid[input_idx] = new_uid()

                            notify_start = edit_start
                            notify_end = edit_end

                            if edit_start == -MAX_I64 or edit_end == MAX_I64:
                                for year in year_files.keys():
                                    if edit_uid == 0:
                                        make_unique_uid(year, input_idx)
                                        continue
                                    if year == 0:
                                        continue
                                    updated = remove_uid(year, edit_uid, input_idx)
                                    if updated is not None:
                                        notify_start = min(notify_start, updated[0])
                                        notify_end = max(notify_end, updated[1])

                                update_start, update_end = modify_edit(0, edit_uid, input_idx)
                                notify_start = min(notify_start, update_start)
                                notify_end = max(notify_end, update_end)
                            else:
                                affected_years = range(*containing_year_range(edit_start / 1000.0, edit_end / 1000.0))
                                for year in year_files.keys():
                                    if edit_uid == 0:
                                        make_unique_uid(year, input_idx)
                                        continue
                                    if year in affected_years:
                                        continue
                                    updated = remove_uid(year, edit_uid, input_idx)
                                    if updated is not None:
                                        notify_start = min(notify_start, updated[0])
                                        notify_end = max(notify_end, updated[1])
                                for year in affected_years:
                                    update_start, update_end = modify_edit(year, edit_uid, input_idx)
                                    notify_start = min(notify_start, update_start)
                                    notify_end = max(notify_end, update_end)

                            await connection.send_notification(edit_directives_notification_key(station),
                                                               notify_start, notify_end)
                            modified_edit_count += 1

                            await connection.set_transaction_status(f"Writing modified edit directives, {(input_idx / input_start_time.shape[0]) * 100.0:.0f}% done")

                        for year, (source_path, edit_file, modified) in year_files.items():
                            if modified and edit_file is None:
                                await connection.remove_file(edit_directives_file_name(station, start_of_year(year) if year != 0 else None))
                                continue
                            edit_file.close()
                            if not modified:
                                continue
                            with open(source_path, "rb") as f:
                                await connection.write_file(edit_directives_file_name(station, start_of_year(year) if year != 0 else None), f)
                        year_files.clear()
                        break
                except LockDenied as ld:
                    _LOGGER.debug("Archive busy: %s", ld.status)
                    if sys.stdout.isatty():
                        if not backoff.has_failed:
                            sys.stdout.write("\n")
                        sys.stdout.write(f"\x1B[2K\rBusy: {ld.status}")
                    await backoff()
                    continue
                finally:
                    for source_path, edit_file, _ in year_files.values():
                        try:
                            edit_file.close()
                        except:
                            pass
                        try:
                            source_path.unlink()
                        except:
                            pass
                    year_files.clear()
                    allocated_uids.clear()

        await connection.shutdown()

    loop.run_until_complete(run())
    loop.close()

    input_file.close()
    print(f"Wrote {modified_edit_count} edit(s) from '{args.file}'")


if __name__ == '__main__':
    main()
