import typing
import numpy as np
from math import floor, ceil
from netCDF4 import Dataset, Group, Variable
from forge.const import MAX_I64
from forge.data.state import is_state_group
from forge.processing.clean.filter import StationFileFilter
from .action import Action
from .condition import Condition


def _walk_data(file: Dataset) -> typing.Iterator[Dataset]:
    def has_time_variable(group: Dataset) -> bool:
        for var in group.variables.values():
            if len(var.dimensions) == 0:
                continue
            if var.dimensions[0] != 'time':
                continue
            return True
        return False

    def walk_inner(group: Dataset, is_parent_state: typing.Optional[bool] = None):
        is_state = is_state_group(group)
        if is_state is None:
            is_state = is_parent_state

        if not is_state and has_time_variable(group):
            yield group

        for g in group.groups.values():
            yield from walk_inner(g, is_state)

    yield from walk_inner(file)


def data_times(data: Dataset) -> typing.Optional[np.ndarray]:
    time_variable = data.variables.get('time')
    if time_variable is not None:
        return time_variable[:].data
    parent = data.parent
    if parent is None:
        return None
    return data_times(parent)


def apply_edit_directives(
        directive_file: Dataset,
        data_files: typing.List[Dataset],
        data_start_ms: int = -MAX_I64,
        data_end_ms: int = MAX_I64,
) -> None:
    directives_root: Group = directive_file.groups.get("edits")
    if directives_root is None:
        return
    start_time: Variable = directives_root.variables.get("start_time")
    end_time: Variable = directives_root.variables.get("end_time")
    if start_time is None or end_time is None:
        return
    start_time: np.ndarray = start_time[...].data
    end_time: np.ndarray = end_time[...].data

    is_deleted: Variable = directives_root.variables.get("deleted")
    active_edits = np.all((
        start_time < data_end_ms,
        end_time > data_start_ms,
        *((is_deleted[...].data == 0,) if is_deleted is not None else ())
    ), axis=0)
    active_edits = np.where(active_edits)[0]
    if active_edits.shape[0] == 0:
        return

    profiles: Variable = directives_root.variables["profile"]
    profile_lookup: typing.Dict[int, str] = dict()
    for code, id_number in profiles.datatype.enum_dict.items():
        profile_lookup[id_number] = code
    profiles: np.ndarray = profiles[...].data

    actions: Variable = directives_root.variables["action_type"]
    action_lookup: typing.Dict[int, typing.Callable[[str], Action]] = dict()
    for code, id_number in actions.datatype.enum_dict.items():
        action_lookup[id_number] = Action.from_code(code)
    actions: np.ndarray = actions[...].data
    action_parameters: np.ndarray = directives_root.variables["action_parameters"][...]

    conditions: Variable = directives_root.variables["condition_type"]
    condition_lookup: typing.Dict[int, typing.Callable[[str], Condition]] = dict()
    for code, id_number in conditions.datatype.enum_dict.items():
        condition_lookup[id_number] = Condition.from_code(code)
    conditions: np.ndarray = conditions[...].data
    condition_parameters: np.ndarray = directives_root.variables["condition_parameters"][...]

    instantiated_edits: typing.List[typing.Tuple[int, int, str, Condition, Action]] = list()
    any_action_prepare: bool = False
    any_condition_prepare: bool = False
    for idx in active_edits:
        edit_start = int(start_time[idx])
        edit_end = int(end_time[idx])
        edit_profile = profile_lookup[int(profiles[idx])]
        edit_action = action_lookup[int(actions[idx])](str(action_parameters[idx]))
        if edit_action.needs_prepare:
            any_action_prepare = True
        edit_condition = condition_lookup[int(conditions[idx])](str(condition_parameters[idx]))
        if edit_condition.needs_prepare:
            any_condition_prepare = True
        instantiated_edits.append((edit_start, edit_end, edit_profile, edit_condition, edit_action))

    if any_condition_prepare:
        for file in data_files:
            for data in _walk_data(file):
                for _, _, _, condition, _ in instantiated_edits:
                    condition.prepare(file, data)

    file_filter: typing.Optional[StationFileFilter] = None

    def get_file_filter():
        nonlocal file_filter

        if file_filter is not None:
            return file_filter

        station = directive_file.variables.get("station_name")
        if station is None:
            file_filter = StationFileFilter.load_station(
                None,
                int(floor(data_start_ms / 1000)),
                int(ceil(data_end_ms / 1000))
            )
            return file_filter

        station = str(station[0]).lower()
        file_filter = StationFileFilter.load_station(
            station,
            int(floor(data_start_ms / 1000)),
            int(ceil(data_end_ms / 1000))
        )
        return file_filter

    for file in data_files:
        for data in _walk_data(file):
            times = None

            if any_action_prepare:
                for _, _, profile, _, action in instantiated_edits:
                    if not action.needs_prepare:
                        continue
                    if action.limit_profile:
                        if not get_file_filter().profile_accepts_file(profile, file):
                            continue
                    if not action.filter_data(file, data):
                        continue
                    if times is None:
                        times = data_times(data)
                    if times is None:
                        continue
                    action.prepare(file, data, times)

            for edit_start, edit_end, profile, condition, action in instantiated_edits:
                if action.limit_profile:
                    if not get_file_filter().profile_accepts_file(profile, file):
                        continue
                if not action.filter_data(file, data):
                    continue
                if times is None:
                    times = data_times(data)
                if times is None:
                    continue
                selection = condition.evaluate(times, edit_start, edit_end)
                if selection is None:
                    continue
                action.apply(file, data, selection)
