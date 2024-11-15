import typing
import asyncio
import logging
import time
import random
import os
import numpy as np
import base64
import struct
from math import floor, ceil, isfinite
from tempfile import TemporaryDirectory, NamedTemporaryFile, mkstemp
from pathlib import Path
from json import loads as from_json, dumps as to_json
from netCDF4 import Dataset, Variable
from starlette.requests import Request
from forge.logicaltime import containing_year_range, start_of_year, start_of_year_ms, round_to_year, year_bounds
from forge.formattime import format_export_time
from forge.const import STATIONS, MAX_I64
from forge.vis.access import AccessUser
from forge.vis.data.stream import DataStream, ArchiveReadStream
from forge.archive.client import edit_directives_lock_key, edit_directives_file_name, edit_directives_notification_key, data_lock_key, data_file_name, index_lock_key, index_file_name
from forge.archive.client.get import read_file_or_nothing
from forge.archive.client.archiveindex import ArchiveIndex
from forge.archive.client.connection import Connection, LockDenied, LockBackoff
from forge.data.enum import remap_enum
from forge.data.state import is_state_group
from forge.data.dimensions import find_dimension_values
from forge.data.structure import edit_directives
from forge.data.structure.editdirectives import edit_file_structure
from forge.processing.clean.passing import apply_pass as archive_apply_pass
from forge.processing.clean.filter import StationFileFilter
from forge.processing.editing.selection import ignore_variable

_LOGGER = logging.getLogger(__name__)


def _sanitize_selection(selection: typing.List[typing.Dict[str, typing.Any]]) -> typing.List[typing.Dict[str, typing.Any]]:
    if not isinstance(selection, list):
        raise ValueError
    result: typing.List[typing.Dict[str, typing.Any]] = list()
    for item in selection:
        if not isinstance(item, dict):
            raise ValueError
        converted: typing.Dict[str, typing.Any] = dict()
        for skey in ("instrument_id", "instrument", "variable_id", "variable_name", "standard_name"):
            value = item.get(skey)
            if not value or not isinstance(value, str):
                continue
            value = str(value).strip()
            if not value:
                continue
            converted[skey] = value
        for lkey in ("require_tags", "exclude_tags"):
            value = item.get(lkey)
            if not value:
                continue
            if isinstance(value, str):
                value = value.split()
            elif not isinstance(value, list):
                raise ValueError
            value = set(value)
            if not value:
                continue
            converted[lkey] = sorted(value)
        for fkey in ("wavelength", ):
            value = item.get(fkey)
            if value is None:
                continue
            value = float(value)
            if not isfinite(value):
                continue
            converted[fkey] = value
        if not converted:
            raise ValueError
        result.append(converted)
    return result


def _sanitize_calibration(calibration: typing.List[float]) -> typing.List[float]:
    if not isinstance(calibration, list):
        raise ValueError
    result: typing.List[float] = list()
    for item in calibration:
        item = float(item)
        if not isfinite(item):
            raise ValueError
        result.append(item)
    return result


def _from_archive_action(code: str, parameters: str) -> typing.Dict[str, typing.Any]:
    code = code.lower()
    if code == 'contaminate':
        return {
            'action': 'contaminate',
        }
    elif code == 'calibration':
        parameters = from_json(parameters)
        return {
            'action': 'calibration',
            'selection': parameters["selection"],
            'calibration': parameters["calibration"],
        }
    elif code == 'recalibrate':
        parameters = from_json(parameters)
        return {
            'action': 'recalibrate',
            'selection': parameters["selection"],
            'calibration': parameters["calibration"],
            'reverse_calibration': parameters["reverse_calibration"],
        }
    elif code == 'flowcorrection':
        parameters = from_json(parameters)
        return {
            'action': 'flow_correction',
            'instrument': parameters["instrument"],
            'calibration': parameters["calibration"],
            'reverse_calibration': parameters["reverse_calibration"],
        }
    elif code == 'sizecutfix':
        parameters = from_json(parameters)
        return {
            'action': 'cut_size',
            'cutsize': parameters["cutsize"],
            'modified_cutsize': parameters["modified_cutsize"],
        }
    elif code == 'abnormaldata':
        parameters = from_json(parameters)
        return {
            'action': 'abnormal_data',
            'episode_type': parameters["episode_type"],
        }

    parameters = from_json(parameters)
    return {
        'action': 'invalidate',
        'selection': parameters["selection"],
    }


def _to_archive_action(action: typing.Dict[str, typing.Any]) -> typing.Tuple[str, typing.Optional[typing.Dict[str, typing.Any]]]:
    if not isinstance(action, dict):
        raise ValueError

    code = action.get('action', 'invalidate').lower()
    if code == 'contaminate':
        return 'Contaminate', None
    elif code == 'calibration':
        return 'Calibration', {
            "selection": _sanitize_selection(action.get('selection')),
            "calibration": _sanitize_calibration(action.get('calibration')),
        }
    elif code == 'recalibrate':
        return 'Recalibrate', {
            "selection": _sanitize_selection(action.get('selection')),
            "calibration": _sanitize_calibration(action.get('calibration')),
            "reverse_calibration": _sanitize_calibration(action.get('reverse_calibration')),
        }
    elif code == 'flow_correction':
        instrument = action.get('instrument')
        if not isinstance(instrument, str):
            raise ValueError
        instrument = str(instrument).strip()
        if not instrument:
            raise ValueError
        return 'FlowCorrection', {
            "instrument": instrument,
            "calibration": _sanitize_calibration(action.get('calibration')),
            "reverse_calibration": _sanitize_calibration(action.get('reverse_calibration')),
        }
    elif code == 'cut_size':
        operate_size = action.get('cutsize')
        if operate_size is not None:
            operate_size = float(operate_size)
            if not isfinite(operate_size):
                operate_size = None
        output_cut = action.get('modified_cutsize')
        if output_cut is not None:
            if isinstance(output_cut, str):
                output_cut = str(output_cut).lower()
                if output_cut != 'invalidate':
                    raise ValueError
            else:
                output_cut = float(output_cut)
                if not isfinite(output_cut):
                    output_cut = None
        return 'SizeCutFix', {
            "cutsize": operate_size,
            "modified_cutsize": output_cut,
        }
    elif code == 'abnormal_data':
        episode_type = action.get('episode_type')
        if not isinstance(episode_type, str):
            raise ValueError
        episode_type = str(episode_type).lower()
        if episode_type not in ('wild_fire', 'dust'):
            raise ValueError
        return 'AbnormalData', {
            "episode_type": episode_type,
        }

    return 'Invalidate', {
        "selection": _sanitize_selection(action.get('selection')),
    }


def _from_archive_condition(code: str, parameters: str) -> typing.Optional[typing.Dict[str, typing.Any]]:
    code = code.lower()
    if code == 'periodic':
        parameters = from_json(parameters)
        return {
            'type': 'periodic',
            'moments': parameters["moments"],
            'interval': parameters["interval"],
            'division': parameters["division"],
        }
    elif code == 'threshold':
        parameters = from_json(parameters)
        return {
            'type': 'threshold',
            'selection': parameters["selection"],
            'lower': parameters.get("lower"),
            'upper': parameters.get("upper"),
        }
    return {'type': 'none'}


def _to_archive_condition(condition: typing.Dict[str, typing.Any]) -> typing.Tuple[str, typing.Optional[typing.Dict[str, typing.Any]]]:
    if condition is None:
        return "None", None
    if not isinstance(condition, dict):
        raise ValueError

    code = condition.get('type', 'none').lower()
    if code == 'periodic':
        moments = condition.get('moments')
        if not isinstance(moments, list):
            raise ValueError
        unique_moments: typing.Set[int] = set()
        for item in moments:
            unique_moments.add(int(item))
        interval = str(condition.get('interval', 'hour')).lower()
        if interval not in ('hour', 'day'):
            interval = 'hour'
        division = str(condition.get('division', 'minute')).lower()
        if division not in ('minute', 'hour'):
            division = 'minute'
        return "Periodic", {
            "moments": sorted(unique_moments),
            "interval": interval,
            "division": division,
        }
    elif code == 'threshold':
        parameters = {
            "selection": _sanitize_selection(condition.get('selection')),
        }
        lower = condition.get('lower')
        if lower is not None:
            lower = float(lower)
            if isfinite(lower):
                parameters['lower'] = lower
        upper = condition.get('upper')
        if upper is not None:
            upper = float(upper)
            if isfinite(upper):
                parameters['upper'] = upper
        return "Threshold", parameters

    return "None", None


def _from_archive_history(history: str) -> typing.List[typing.Dict[str, typing.Any]]:
    def format_time(ts: float) -> str:
        if not ts or not isfinite(ts) or ts == MAX_I64 or ts == -MAX_I64:
            return "∞"
        return format_export_time(ts / 1000.0)

    if not history:
        return []
    result: typing.List[typing.Dict[str, typing.Any]] = list()
    for entry in from_json(history):
        actions = []

        if entry.get('created'):
            actions.append("Created")

        deleted = entry.get('deleted')
        if deleted is not None:
            if deleted:
                actions.append("Deleted")
            else:
                actions.append("Restored")

        start_change, end_change = entry.get('changed_start_time', False), entry.get('changed_end_time', False)
        if start_change is not False or end_change is not False:
            actions.append(f"Bounds change from {format_time(start_change)} - {format_time(end_change)}")

        if entry.get('changed_action_type'):
            actions.append("Action changed")
        elif entry.get('changed_action_parameters', False) is not False:
            actions.append("Action parameters changed")

        if entry.get('changed_condition_type'):
            actions.append("Condition changed")
        elif entry.get('changed_condition_parameters', False) is not False:
            actions.append("Condition parameters changed")

        prior = entry.get('changed_profile')
        if prior:
            prior = str(prior).title()
            actions.append(f"Profile changed from {prior}")

        prior = entry.get('changed_author')
        if prior:
            prior = str(prior)
            actions.append(f"Author changed from {prior}")

        if entry.get('changed_comment'):
            actions.append("Comment changed")

        result.append({
            'time_epoch_ms': entry['time_epoch_ms'],
            'user': entry.get('user_name') or entry.get('user_id'),
            'operation': ".\n".join(actions),
        })
    return result


def _from_archive_edit(
        start_epoch_ms: int, end_epoch_ms: int, modified_epoch_ms: int, unique_id: int,
        is_deleted: bool, profile: str, reading_profile: str,
        action: str, action_parameters: str,
        condition: str, condition_parameters: str,
        author: str, comment: str, history: str
) -> typing.Dict[str, typing.Any]:
    result = {
        '_id': base64.b64encode(
            struct.pack('<qqQ', start_epoch_ms, end_epoch_ms, unique_id)
        ).decode('ascii'),
        'start_epoch_ms': start_epoch_ms if start_epoch_ms != -MAX_I64 else None,
        'end_epoch_ms': end_epoch_ms if end_epoch_ms != MAX_I64 else None,
        'modified_epoch_ms': modified_epoch_ms if modified_epoch_ms != 0 else None,
        'author': author,
        'comment': comment,
        'other_type': profile.lower() != reading_profile.lower(),
        'type': profile.title(),
        'deleted': is_deleted,
        'history': _from_archive_history(history),
        'condition': _from_archive_condition(condition, condition_parameters),
    }
    for k, v in _from_archive_action(action, action_parameters).items():
        result[k] = v
    return result


class _EditReadStream(ArchiveReadStream):
    def __init__(self, station: str, profile: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)
        self.station = station.lower()
        self.profile = profile.lower()
        self.start_epoch_ms = start_epoch_ms
        self.end_epoch_ms = end_epoch_ms
        self._edit_file_storage: typing.Optional[TemporaryDirectory] = None
        year_start, year_end = containing_year_range(start_epoch_ms / 1000.0, end_epoch_ms / 1000.0)
        self._year_start = year_start
        self._year_end = year_end
        self._seen_multiyear_edits: typing.Set[typing.Tuple[int, int, int]] = set()

    @property
    def connection_name(self) -> str:
        return "read edits"

    async def acquire_locks(self) -> None:
        lock_start = int(floor(start_of_year(self._year_start) * 1000))
        lock_end = int(ceil(start_of_year(self._year_end) * 1000))
        await self.connection.lock_read(edit_directives_lock_key(self.station), lock_start, lock_end)

    async def with_locks_held(self) -> None:
        self._edit_file_storage = TemporaryDirectory()
        destination = Path(self._edit_file_storage.name)
        await read_file_or_nothing(
            self.connection,
            edit_directives_file_name(self.station, None),
            destination
        )
        for year in range(self._year_start, self._year_end):
            await read_file_or_nothing(
                self.connection,
                edit_directives_file_name(self.station, start_of_year(year)),
                destination
            )

    async def _stream_file(self, file: Dataset) -> None:
        directives_root = file.groups["edits"]
        profiles = directives_root.variables["profile"]
        profile_lookup: typing.Dict[int, str] = dict()
        for code, id_number in profiles.datatype.enum_dict.items():
            profile_lookup[id_number] = code

        start_time: Variable = directives_root.variables["start_time"]
        end_time: Variable = directives_root.variables["end_time"]
        start_time: np.ndarray = start_time[...].data
        end_time: np.ndarray = end_time[...].data
        matching_edits = np.all((
            start_time < self.end_epoch_ms,
            end_time > self.start_epoch_ms,
        ), axis=0)

        action = directives_root.variables["action_type"]
        action_lookup: typing.Dict[int, str] = dict()
        for code, id_number in action.datatype.enum_dict.items():
            action_lookup[id_number] = code

        condition = directives_root.variables["condition_type"]
        condition_lookup: typing.Dict[int, str] = dict()
        for code, id_number in condition.datatype.enum_dict.items():
            condition_lookup[id_number] = code

        modified_time = directives_root.variables["modified_time"]
        unique_id = directives_root.variables["unique_id"]
        edit_deleted = directives_root.variables["deleted"]
        action_parameters = directives_root.variables["action_parameters"]
        condition_parameters = directives_root.variables["condition_parameters"]
        author = directives_root.variables["author"]
        comment = directives_root.variables["comment"]
        history = directives_root.variables["history"]

        for idx in np.where(matching_edits)[0]:
            edit_start = int(start_time[idx])
            edit_end = int(end_time[idx])
            edit_uid = int(unique_id[idx])

            if edit_start != -MAX_I64 and edit_end != MAX_I64:
                year_start, year_end = round_to_year(edit_start / 1000.0, edit_end / 1000.0)
                seen_id = (year_start, year_end, edit_uid)
                if seen_id in self._seen_multiyear_edits:
                    continue
                self._seen_multiyear_edits.add(seen_id)

            await self.send(_from_archive_edit(
                edit_start, edit_end, int(modified_time[idx]), edit_uid,
                int(edit_deleted[idx]) != 0, profile_lookup[int(profiles[idx])], self.profile,
                action_lookup[int(action[idx])], str(action_parameters[idx]),
                condition_lookup[int(condition[idx])], str(condition_parameters[idx]),
                str(author[idx]), str(comment[idx]), str(history[idx])
            ))

    async def _stream_edit_files(self) -> None:
        for file in Path(self._edit_file_storage.name).iterdir():
            if not file.is_file():
                continue
            _LOGGER.debug("Sending edits from file: %s", file.name)
            try:
                file = Dataset(file, 'r')
                await self._stream_file(file)
            finally:
                file.close()
            # Explicitly yield, since the send may just be queueing things
            await asyncio.sleep(0)

    async def run(self) -> None:
        try:
            await super().run()
            await self._stream_edit_files()
        finally:
            if self._edit_file_storage:
                self._edit_file_storage.cleanup()
                self._edit_file_storage = None


def read_edits(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
               send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    if station not in STATIONS:
        return None
    profile = mode_name.split('-', 1)[0]
    return _EditReadStream(station, profile, start_epoch_ms, end_epoch_ms, send)


def edit_writable(user: AccessUser, station: str, mode_name: str, directive: typing.Dict[str, typing.Any]) -> bool:
    # All verification happens during save
    return True


async def _apply_edit_save(
        user: AccessUser, station: str, profile: str,
        directive: typing.Dict[str, typing.Any], connection: Connection,
        original_start: typing.Optional[int], original_end: typing.Optional[int],
        original_unique_id: typing.Optional[int],
        updated_start: int, updated_end: int,
) -> typing.Dict[str, typing.Any]:
    modification_time = round(time.time() * 1000)
    notify_start = updated_start
    notify_end = updated_end
    updated_unique_id = 0

    updated_action, updated_action_parameters = _to_archive_action(directive)
    updated_condition, updated_condition_parameters = _to_archive_condition(directive.get('condition'))
    updated_author = str(directive.get('author', ''))
    updated_comment = str(directive.get('comment', '')).strip()
    updated_deleted = bool(directive.get('deleted', False))
    if not updated_author:
        updated_author = user.initials
    if not updated_author:
        updated_author = user.display_name
    if not updated_author:
        updated_author = user.display_id

    original_profile: typing.Optional[str] = None
    first_history: typing.Optional[str] = None

    def updated_history(original_file: Dataset, original_idx: int) -> str:
        original_root = original_file.groups["edits"]
        history = str(original_root['history'][original_idx])
        if history:
            history = from_json(history)
        else:
            history = []

        item = {
            'time_epoch_ms': modification_time,
            'user_id': user.display_id,
            'user_name': user.display_name,
        }

        prior_deleted = bool(original_root['deleted'][original_idx])
        if updated_deleted != prior_deleted:
            item['deleted'] = updated_deleted

        prior = int(original_root['start_time'][original_idx])
        if prior != updated_start:
            if prior == -MAX_I64:
                prior = None
            item['changed_start_time'] = prior
        prior = int(original_root['end_time'][original_idx])
        if prior != updated_end:
            if prior == MAX_I64:
                prior = None
            item['changed_end_time'] = prior

        for var, updated in (
                ('author', updated_author),
                ('comment', updated_author)
        ):
            prior = str(original_root[var][original_idx])
            if prior == updated:
                continue
            item['changed_' + var] = prior

        for var, updated in (
                ("action_parameters", updated_action_parameters),
                ("condition_parameters", updated_condition_parameters),
        ):
            prior = str(original_root[var][original_idx])
            if prior:
                prior = from_json(prior)
                if prior == updated:
                    continue
            elif not updated:
                continue
            else:
                prior = None
            item['changed_' + var] = prior

        for var, updated in (
                ("profile", original_profile or profile),
                ("action_type", updated_action),
                ("condition_type", updated_condition),
        ):
            prior_var = original_root[var]
            prior = int(prior_var[original_idx])
            for check, value in prior_var.datatype.enum_dict.items():
                if value == prior:
                    prior = check
                    break
            else:
                prior = ""
            if prior.lower() == updated.lower():
                continue
            item['changed_' + var] = prior

        history.append(item)
        return to_json(history, sort_keys=True)

    def created_history() -> str:
        return to_json([
            {
                'time_epoch_ms': modification_time,
                'user_id': user.display_id,
                'user_name': user.display_name,
                'created': True,
            }
        ], sort_keys=True)

    def write_edit(file: Dataset, unique_id: int, history: str) -> None:
        nonlocal first_history
        if not first_history:
            first_history = history

        write_profile = original_profile or profile
        if not user.allow_mode(station, write_profile.lower() + '-editing', write=True):
            raise PermissionError

        output_root = file.groups["edits"]
        output_idx = output_root.dimensions["index"].size

        for var, value in (
                ("start_time", updated_start),
                ("end_time", updated_end),
                ("modified_time", modification_time),
                ("unique_id", unique_id),
                ("deleted", 1 if updated_deleted else 0),
                ("action_parameters", to_json(updated_action_parameters, sort_keys=True) if updated_action_parameters else ""),
                ("condition_parameters", to_json(updated_condition_parameters, sort_keys=True) if updated_condition_parameters else ""),
                ("author", updated_author),
                ("comment", updated_comment),
                ("history", history),
        ):
            output_root.variables[var][output_idx] = value

        for var, value in (
                ("profile", write_profile),
                ("action_type", updated_action),
                ("condition_type", updated_condition),
        ):
            var = output_root.variables[var]
            var[output_idx] = var.datatype.enum_dict[value]

    with TemporaryDirectory() as tmpdir:
        modified_files: typing.Dict[int, typing.Optional[Dataset]] = dict()
        modified_history: typing.Dict[int, str] = dict()
        allocated_unique_ids: typing.Set[int] = set()
        if original_unique_id is not None:
            allocated_unique_ids.add(original_unique_id)

        async def fetch_file(start_ms: int) -> typing.Optional[Dataset]:
            existing = modified_files.get(start_ms, False)
            if existing is not False:
                return existing
            fd, destination_name = mkstemp(suffix='.nc', dir=tmpdir)
            with os.fdopen(fd, 'wb') as f:
                try:
                    await connection.read_file(
                        edit_directives_file_name(station, start_ms / 1000.0 if start_ms != -MAX_I64 else None),
                        f
                    )
                except FileNotFoundError:
                    return None
            data = Dataset(destination_name, 'r+')
            modified_files[start_ms] = data
            return data

        def construct_modified(start_ms: int, profiles: typing.Iterable[str]) -> Dataset:
            fd, modified_name = mkstemp(suffix='.nc', dir=tmpdir)
            os.close(fd)
            output_file = Dataset(modified_name, 'w', format='NETCDF4')
            modified_files[start_ms] = output_file
            if start_ms != -MAX_I64:
                file_start, file_end = year_bounds(year)
            else:
                file_start = None
                file_end = None
            edit_directives(output_file, station, file_start, file_end)
            edit_file_structure(output_file, sorted(profiles))
            return output_file

        def remove_existing(source: Dataset, start_ms: int) -> bool:
            nonlocal notify_start
            nonlocal notify_end
            nonlocal original_profile

            existing_root = source.groups["edits"]
            existing_uid = existing_root.variables["unique_id"][...].data
            hit = np.where(existing_uid == original_unique_id)[0]
            if hit.shape[0] == 0:
                return False
            assert hit.shape[0] == 1
            remove_index = int(hit[0])
            exiting_start_time = int(existing_root.variables["start_time"][...].data[remove_index])
            exiting_end_time = int(existing_root.variables["end_time"][...].data[remove_index])
            notify_start = min(notify_start, exiting_start_time)
            notify_end = max(notify_end, exiting_end_time)

            modified_history[start_ms] = updated_history(source, remove_index)

            existing_profiles = existing_root.variables["profile"]
            existing_profiles_lookup = existing_profiles.datatype.enum_dict
            existing_profiles = existing_profiles[...].data
            remove_profile_value = int(existing_profiles[...].data[remove_index])
            for check, value in existing_profiles_lookup.items():
                if value == remove_profile_value:
                    if not user.allow_mode(station, check.lower() + '-editing', write=True):
                        raise PermissionError
                    if original_profile is None:
                        original_profile = check
                    break

            if existing_uid.shape[0] == 1:
                filename = source.filepath()
                source.close()
                Path(filename).unlink()
                modified_files[start_ms] = None
                return True

            all_profiles: typing.Set[str] = set()
            if original_profile:
                all_profiles.add(original_profile)
            else:
                all_profiles.add(profile)
            for check_profile, check_value in existing_profiles_lookup.items():
                if not np.any(existing_profiles == check_value):
                    continue
                all_profiles.add(check_profile)

            output_file = construct_modified(start_ms, all_profiles)
            output_root = output_file.groups["edits"]

            for var in ("start_time", "end_time", "modified_time", "unique_id"):
                input_var = existing_root.variables[var]
                output_var = output_root.variables[var]
                if remove_index != 0:
                    output_var[:remove_index] = input_var[:remove_index]
                if remove_index != input_var.shape[0] - 1:
                    output_var[remove_index:] = input_var[remove_index + 1:]
            for var in ("action_parameters", "condition_parameters", "author", "comment", "history"):
                input_var = existing_root.variables[var]
                output_var = output_root.variables[var]
                output_index = 0
                for input_idx in range(input_var.shape[0]):
                    if input_idx == remove_index:
                        continue
                    output_var[output_index] = input_var[input_idx]
                    output_index += 1
            for var in ("profile", "action_type", "condition_type", "deleted"):
                remap_enum(existing_root.variables[var], output_root.variables[var], remove_index=remove_index)

            filename = source.filepath()
            source.close()
            Path(filename).unlink()
            return True

        def file_contains_unique_id(source: Dataset, uid: int) -> bool:
            existing_root = source.groups["edits"]
            existing_uid = existing_root.variables["unique_id"][...].data
            return bool(np.any(existing_uid == uid))

        def new_unique_id() -> int:
            while True:
                uid = random.randint(1, 1 << 64)
                if uid in allocated_unique_ids:
                    continue
                for file in modified_files.values():
                    if file is None:
                        continue
                    if file_contains_unique_id(file, uid):
                        allocated_unique_ids.add(uid)
                        break
                else:
                    allocated_unique_ids.add(uid)
                    return uid

        if original_unique_id:
            if original_start == -MAX_I64 or original_end == MAX_I64:
                data = await fetch_file(-MAX_I64)
                if data is None:
                    raise FileNotFoundError
                if not remove_existing(data, -MAX_I64):
                    raise FileNotFoundError
            else:
                any_hit = False
                for year in range(*containing_year_range(original_start / 1000.0, original_end / 1000.0)):
                    file_start_ms = start_of_year_ms(year)
                    data = await fetch_file(file_start_ms)
                    if data is None:
                        continue
                    if remove_existing(data, file_start_ms):
                        any_hit = True
                if not any_hit:
                    # Rather than aborting for a single missed year, just require that any hit to
                    # allow for recovery from wierd corruption (shrinking and edit and modifying the
                    # _id to cause partial remove)
                    raise FileNotFoundError

        updated_unique_id = new_unique_id()

        if updated_start == -MAX_I64 or updated_end == MAX_I64:
            output_file = await fetch_file(-MAX_I64)
            if output_file is None:
                output_file = construct_modified(-MAX_I64, [profile])
                _LOGGER.debug("Created unlimited edits file for year %s", station)
            else:
                if file_contains_unique_id(output_file, updated_unique_id):
                    updated_unique_id = new_unique_id()

            history = modified_history.get(-MAX_I64)
            if history is None:
                history = created_history()
            write_edit(output_file, updated_unique_id, history)
        else:
            year_start, year_end = containing_year_range(updated_start / 1000.0, updated_end / 1000.0)
            for year in range(year_start, year_end):
                file_start_ms = start_of_year_ms(year)
                output_file = await fetch_file(file_start_ms)
                if output_file is not None:
                    if file_contains_unique_id(output_file, updated_unique_id):
                        updated_unique_id = new_unique_id()
            for year in range(year_start, year_end):
                file_start_ms = start_of_year_ms(year)
                output_file = modified_files.get(file_start_ms)
                if output_file is None:
                    output_file = construct_modified(file_start_ms, [profile])
                    _LOGGER.debug("Created new edits file for %s/%d", station, year)

                history = modified_history.get(file_start_ms)
                if history is None:
                    history = created_history()
                write_edit(output_file, updated_unique_id, history)

        for file_start_ms, output_file in modified_files.items():
            archive_name = edit_directives_file_name(
                station,
                file_start_ms / 1000.0 if file_start_ms != -MAX_I64 else None
            )
            if output_file is None:
                await connection.remove_file(archive_name)
            else:
                filename = output_file.filepath()
                output_file.close()
                with open(filename, "rb") as f:
                    await connection.write_file(archive_name, f)
                try:
                    os.unlink(filename)
                except:
                    pass

    await connection.send_notification(edit_directives_notification_key(station),
                                       notify_start, notify_end)

    return _from_archive_edit(
        updated_start, updated_end, modification_time, updated_unique_id,
        updated_deleted, original_profile or profile, profile,
        updated_action, to_json(updated_action_parameters or {}),
        updated_condition, to_json(updated_condition_parameters or {}),
        updated_author, updated_comment, first_history or "[]",
    )


async def edit_save(user: AccessUser, station: str, mode_name: str,
                    directive: typing.Dict[str, typing.Any]) -> typing.Optional[typing.Dict[str, typing.Any]]:
    if station not in STATIONS:
        return None
    profile = mode_name.split('-', 1)[0]

    original_start_ms = None
    original_end_ms = None
    original_unique_id = None
    if '_id' in directive:
        try:
            original_start_ms, original_end_ms, original_unique_id = struct.unpack('<qqQ', base64.b64decode(directive['_id']))
        except:
            _LOGGER.debug(f"Invalid original edit from {user.display_id} on {station}:{profile}", exc_info=True)
            return None
        if original_unique_id == 0:
            _LOGGER.debug(f"Invalid original UID from {user.display_id} on {station}:{profile}")
            return None
        if original_start_ms >= original_end_ms:
            _LOGGER.debug(f"Invalid original time bounds from {user.display_id} on {station}:{profile}")
            return None

        # 1970-2200 as sanity limits
        if original_start_ms != -MAX_I64 and original_start_ms <= 0:
            _LOGGER.debug(f"Invalid original start time from {user.display_id} on {station}:{profile}")
            return None
        if original_end_ms != MAX_I64 and original_end_ms > 7258118400_000:
            _LOGGER.debug(f"Invalid original end time from {user.display_id} on {station}:{profile}")
            return None

    updated_start = directive.get('start_epoch_ms')
    updated_end = directive.get('end_epoch_ms')
    updated_start = floor(updated_start) if updated_start else -MAX_I64
    updated_end = ceil(updated_end) if updated_end else MAX_I64
    if updated_start >= updated_end:
        _LOGGER.debug(f"Invalid time bounds from {user.display_id} on {station}:{profile}")
        return None
    if updated_start != -MAX_I64 and updated_start <= 0:
        _LOGGER.debug(f"Invalid start time from {user.display_id} on {station}:{profile}")
        return None
    if updated_end != MAX_I64 and updated_end > 7258118400_000:
        _LOGGER.debug(f"Invalid end time from {user.display_id} on {station}:{profile}")
        return None

    if updated_start == -MAX_I64 or (original_start_ms is not None and original_start_ms == -MAX_I64):
        lock_start = -MAX_I64
    else:
        lock_start = updated_start
        if original_start_ms is not None:
            lock_start = min(lock_start, original_start_ms)
        year_number = time.gmtime(lock_start / 1000.0).tm_year
        lock_start = start_of_year_ms(year_number)

    if updated_end == MAX_I64 or (original_end_ms is not None and original_end_ms == MAX_I64):
        lock_end = MAX_I64
    else:
        raw_lock_end = updated_end
        if original_end_ms is not None:
            raw_lock_end = max(raw_lock_end, original_end_ms)
        year_number = time.gmtime(raw_lock_end / 1000.0).tm_year
        lock_end = start_of_year_ms(year_number)
        if lock_end < raw_lock_end:
            year_number += 1
            lock_end = start_of_year_ms(year_number)

    async with await Connection.default_connection("save edit", use_environ=False) as connection:
        backoff = LockBackoff()
        while True:
            try:
                async with connection.transaction(True):
                    await connection.lock_write(edit_directives_lock_key(station), lock_start, lock_end)
                    try:
                        return await _apply_edit_save(
                            user, station, profile, directive, connection,
                            original_start_ms, original_end_ms, original_unique_id,
                            updated_start, updated_end
                        )
                    except:
                        _LOGGER.debug(f"Error saving directive for {user.display_id} on {station}:{profile}",
                                      exc_info=True)
                        return None
            except LockDenied:
                await backoff()
                continue


class _AvailableReadStream(ArchiveReadStream):
    ARCHIVE = "raw"

    class _Index(ArchiveIndex):
        def apply_filter(self, profile: str, file_filter: StationFileFilter) -> typing.Set[str]:
            check_instruments: typing.Set[str] = set()
            for instrument_id, tags in self.tags.items():
                accept = file_filter.profile_filter_tags(profile, tags)
                if accept is not None and not accept:
                    # If it's ambiguous, then we have to load the files anyway
                    continue
                check_instruments.add(instrument_id)
            return check_instruments

    class _AvailableVariableID:
        def __init__(self, variable_id: str):
            self.variable_id = variable_id
            self.wavelengths: typing.Set[float] = set()
            self.long_name: typing.Optional[str] = None

        def _integrate_wavelengths(self, var: Variable) -> None:
            if 'wavelength' not in var.dimensions:
                return
            _, wavelengths = find_dimension_values(var.group(), 'wavelength')
            for wl in wavelengths[:].data:
                wl = float(wl)
                if not isfinite(wl):
                    continue
                self.wavelengths.add(wl)

        def integrate_variable(self, var: Variable) -> None:
            self._integrate_wavelengths(var)
            try:
                self.long_name = str(var.long_name).strip()
            except (AttributeError, ValueError, TypeError):
                pass

        async def write(self, send: typing.Callable[[typing.Dict], typing.Awaitable[None]], instrument: "_AvailableReadStream._AvailableInstrument") -> None:
            selection: typing.Dict[str, typing.Any] = {
                'type': 'variable_id',
                'instrument_id': instrument.instrument_id,
                'variable_id': self.variable_id,
            }
            if self.wavelengths:
                selection['wavelengths'] = sorted(self.wavelengths)
            if self.long_name:
                selection['description'] = self.long_name
            if instrument.instrument:
                selection['instrument'] = instrument.instrument
            if instrument.manufacturer:
                selection['manufacturer'] = instrument.manufacturer
            if instrument.model:
                selection['model'] = instrument.model
            if instrument.serial_number:
                selection['serial_number'] = instrument.serial_number
            await send(selection)

    class _AvailableInstrument:
        def __init__(self, instrument_id: str):
            self.instrument_id = instrument_id
            self.instrument: typing.Optional[str] = None
            self.manufacturer: typing.Optional[str] = None
            self.model: typing.Optional[str] = None
            self.serial_number: typing.Optional[str] = None
            self._variable_id: typing.Dict[str, "_AvailableReadStream._AvailableVariableID"] = dict()

        def integrate_file(self, root: Dataset) -> None:
            instrument = getattr(root, 'instrument', None)
            if instrument:
                self.instrument = str(instrument)

            instrument_data = root.groups.get("instrument")
            if instrument_data is not None:
                try:
                    self.manufacturer = str(instrument_data.variables["manufacturer"][0])
                except (KeyError, AttributeError, TypeError, ValueError):
                    pass
                try:
                    self.model = str(instrument_data.variables["model"][0])
                except (KeyError, AttributeError, TypeError, ValueError):
                    pass
                try:
                    self.serial_number = str(instrument_data.variables["serial_number"][0])
                except (KeyError, AttributeError, TypeError, ValueError):
                    pass

        def _integrate_variable_id(self, var: Variable) -> None:
            variable_id = getattr(var, 'variable_id', None)
            if not variable_id:
                return
            v = self._variable_id.get(variable_id)
            if v is None:
                v = _AvailableReadStream._AvailableVariableID(variable_id)
                self._variable_id[variable_id] = v
            v.integrate_variable(var)

        def integrate_variable(self, var: Variable) -> None:
            if ignore_variable(var):
                return
            self._integrate_variable_id(var)

        async def write(self, send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> None:
            for v in self._variable_id.values():
                await v.write(send, self)

    class _AvailableTracking:
        def __init__(self):
            self._instrument_id: typing.Dict[str, "_AvailableReadStream._AvailableInstrument"] = dict()

        def instrument(self, instrument_id) -> "_AvailableReadStream._AvailableInstrument":
            i = self._instrument_id.get(instrument_id)
            if i is None:
                i = _AvailableReadStream._AvailableInstrument(instrument_id)
                self._instrument_id[instrument_id] = i
            return i

        async def write(self, send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> None:
            _LOGGER.debug("Sending available data for %d instruments", len(self._instrument_id))
            for i in self._instrument_id.values():
                await i.write(send)

    def __init__(self, station: str, profile: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        super().__init__(send)
        self.station = station.lower()
        self.profile = profile.lower()
        self.start_epoch_ms = start_epoch_ms
        self.end_epoch_ms = end_epoch_ms
        year_start, year_end = containing_year_range(start_epoch_ms / 1000.0, end_epoch_ms / 1000.0)
        self._year_start = year_start
        self._year_end = year_end
        self._tracking = self._AvailableTracking()

    @property
    def connection_name(self) -> str:
        return "read available"

    async def acquire_locks(self) -> None:
        lock_start = int(floor(start_of_year(self._year_start) * 1000))
        lock_end = int(ceil(start_of_year(self._year_end) * 1000))
        await self.connection.lock_read(index_lock_key(self.station, self.ARCHIVE), lock_start, lock_end)
        lock_start = int(floor(self.start_epoch_ms / (24 * 60 * 60 * 1000)) * 24 * 60 * 60 * 1000)
        lock_end = int(ceil(self.end_epoch_ms / (24 * 60 * 60 * 1000)) * 24 * 60 * 60 * 1000)
        await self.connection.lock_read(data_lock_key(self.station, self.ARCHIVE), lock_start, lock_end)

    async def _load_index(self, year: int) -> typing.Optional["_AvailableReadStream._Index"]:
        try:
            index_contents = await self.connection.read_bytes(index_file_name(self.station, self.ARCHIVE, start_of_year(year)))
        except FileNotFoundError:
            return None
        try:
            return self._Index(index_contents)
        except RuntimeError:
            _LOGGER.warning(f"Invalid index for {self.station.upper()}/{year}", exc_info=True)
        return None

    async def _process_file(self, file_filter: StationFileFilter, root: Dataset) -> None:
        if not file_filter.profile_accepts_file(self.profile, root):
            return

        instrument_id = getattr(root, 'instrument_id', None)
        if not instrument_id:
            return
        tracking_instrument = self._tracking.instrument(instrument_id)
        tracking_instrument.integrate_file(root)

        def walk_group(group: Dataset, is_parent_state: typing.Optional[bool] = None) -> None:
            is_state = is_state_group(group)
            if is_state is None:
                is_state = is_parent_state

            if not is_state:
                for var in group.variables.values():
                    tracking_instrument.integrate_variable(var)

            for child in group.groups.values():
                walk_group(child, is_state)

        walk_group(root)

    async def with_locks_held(self) -> None:
        file_filter = StationFileFilter.load_station(
            self.station,
            int(floor(self.start_epoch_ms / 1000)),
            int(ceil(self.end_epoch_ms / 1000))
        )

        file_start_ms = int(floor(self.start_epoch_ms / (24 * 60 * 60 * 1000)) * 24 * 60 * 60 * 1000)
        file_end_ms = int(ceil(self.end_epoch_ms / (24 * 60 * 60 * 1000)) * 24 * 60 * 60 * 1000)

        for year in range(self._year_start, self._year_end):
            index = await self._load_index(year)
            if index is None:
                continue

            file_start_ms = max(file_start_ms, int(floor(start_of_year(year) * 1000)))
            file_end_ms = min(file_end_ms, int(ceil(start_of_year(year+1) * 1000)))

            for instrument in index.apply_filter(self.profile, file_filter):
                for file_time_ms in range(file_start_ms, file_end_ms, 24 * 60 * 60 * 1000):
                    with NamedTemporaryFile(suffix=".nc") as archive_file:
                        try:
                            await self.connection.read_file(
                                data_file_name(
                                    self.station, self.ARCHIVE, instrument, file_time_ms / 1000.0
                                ), archive_file
                            )
                        except FileNotFoundError:
                            continue
                        archive_file.flush()
                        try:
                            data_file = Dataset(archive_file.name, 'r')
                            await self._process_file(file_filter, data_file)
                        finally:
                            data_file.close()

    async def run(self) -> None:
        await super().run()
        await self._tracking.write(self.send)


def available_selections(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
                         send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    if station not in STATIONS:
        return None
    profile = mode_name.split('-', 1)[0]
    return _AvailableReadStream(station, profile, start_epoch_ms, end_epoch_ms, send)


async def apply_pass(request: Request, station: str, mode_name: str, start_epoch_ms: int,
                     end_epoch_ms: int, comment: typing.Optional[str] = None) -> None:
    station = station.lower()
    if station not in STATIONS:
        raise ValueError(f"Invalid station {station}")
    profile = mode_name.split('-', 1)[0].lower()
    if not profile:
        raise ValueError(f"Invalid profile {profile}")

    auxiliary = {
        "type": "web",
        "id": request.user.display_id,
        "name": request.user.display_name,
        "initials": request.user.initials,
        "host": request.client.host,
    }

    async with await Connection.default_connection("pass data", use_environ=False) as connection:
        backoff = LockBackoff()
        while True:
            try:
                async with connection.transaction(True):
                    await archive_apply_pass(
                        connection, station, profile,
                        start_epoch_ms / 1000.0, end_epoch_ms / 1000.0,
                        comment.strip() or "", auxiliary,
                    )
                break
            except LockDenied as ld:
                _LOGGER.debug("Archive busy: %s", ld.status)
                await backoff()
                continue
