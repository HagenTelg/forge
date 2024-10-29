import typing
import re
import numpy as np
from math import floor, ceil, nan
from pathlib import Path
from shutil import copy
from netCDF4 import Dataset, Variable
from forge.timeparse import parse_iso8601_time
from forge.processing.station.lookup import station_data
from forge.data.flags import parse_flags


class StationContamination:
    def is_contamination_flag(self, flag_bit: int, flag_name: str) -> bool:
        return flag_name.startswith("data_contamination_")

    def variable_affected(self, variable: Variable) -> bool:
        if variable.name == 'system_flags':
            return False
        return True


def _invalidate_group(group: Dataset, apply: StationContamination) -> None:
    for g in group.groups.values():
        _invalidate_group(g, apply)
    flags = group.variables.get('system_flags')
    if flags is None or len(flags.dimensions) == 0 or flags.dimensions[0] != 'time':
        return
    contamination_mask = 0
    for bit, flag in parse_flags(flags).items():
        if not apply.is_contamination_flag(bit, flag):
            continue
        contamination_mask |= bit
    if contamination_mask == 0:
        return
    contaminated_points = np.bitwise_and(flags[:].data, contamination_mask) != 0
    if not np.any(contaminated_points):
        return

    for var in group.variables.values():
        if var.name == 'time':
            continue
        if len(var.dimensions) == 0 or var.dimensions[0] != 'time':
            continue
        if not apply.variable_affected(var):
            continue
        try:
            fill_value = var._FillValue
        except AttributeError:
            if np.issubdtype(var.dtype, np.floating):
                fill_value = nan
            else:
                fill_value = 0
        var[contaminated_points] = fill_value


def _analyze_file(
        file: Dataset,
        station: typing.Optional[str] = None,
        tags: typing.Optional[typing.Set[str]] = None,
) -> typing.Tuple[typing.Optional[str], typing.Set[str], typing.Optional[int], typing.Optional[int]]:
    if station is None:
        station_var = file.variables.get("station_name")
        if station_var is not None:
            station = str(station_var[0])
    if not station:
        station = None

    time_coverage_start = getattr(file, 'time_coverage_start', None)
    if time_coverage_start is not None:
        time_coverage_start = int(floor(parse_iso8601_time(str(time_coverage_start)).timestamp()))
    time_coverage_end = getattr(file, 'time_coverage_end', None)
    if time_coverage_end is not None:
        time_coverage_end = int(ceil(parse_iso8601_time(str(time_coverage_end)).timestamp()))

    if tags is None:
        tags = set(str(getattr(file, 'forge_tags', "")).split())

    return station, tags, time_coverage_start, time_coverage_end


def copy_contaminated(
        file: Dataset,
        station: typing.Optional[str] = None,
        tags: typing.Optional[typing.Set[str]] = None,
) -> typing.Optional[str]:
    instrument_id = getattr(file, 'instrument_id', None)
    if not instrument_id:
        return None

    station, tags, start, end = _analyze_file(file, station, tags)

    if station:
        output_instrument = station_data(station, 'contamination', 'keep_contaminated')(
            station, instrument_id, tags, start, end
        )
    else:
        from forge.processing.station.default.contamination import keep_contaminated as keep_contaminated_default
        output_instrument = keep_contaminated_default("nil", instrument_id, tags, start, end)

    return output_instrument


def invalidate_contamination(
        file: Dataset,
        station: typing.Optional[str] = None,
        tags: typing.Optional[typing.Set[str]] = None,
) -> None:
    station, tags, start, end = _analyze_file(file, station, tags)

    if station:
        apply = station_data(station, 'contamination', 'apply')(station, tags, start, end)
    else:
        from forge.processing.station.default.contamination import apply as apply_default
        apply = apply_default("nil", tags, start, end)

    _invalidate_group(file, apply)
