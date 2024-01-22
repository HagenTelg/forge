import typing
import numpy as np
from netCDF4 import Dataset, Variable, Group
from forge.const import MAX_I64
from .timeseries import variable_coordinates


def pass_bounds(target: Dataset) -> typing.Tuple[Variable, Variable]:
    dim = target.createDimension("index", None)

    start = target.createVariable("start_time", "i8", ("index",), fill_value=-MAX_I64)
    variable_coordinates(target, start)
    start.long_name = "inclusive start time of the data pass"
    start.standard_name = "time"
    start.coverage_content_type = "coordinate"
    start.units = "milliseconds since 1970-01-01 00:00:00"

    end = target.createVariable("end_time", "i8", ("index",), fill_value=MAX_I64)
    variable_coordinates(target, end)
    end.long_name = "exclusive end time of the data pass"
    end.standard_name = "time"
    end.coverage_content_type = "coordinate"
    end.units = "milliseconds since 1970-01-01 00:00:00"

    return start, end


def pass_time(target: Dataset) -> Variable:
    var = target.createVariable("pass_time", "i8", ("index",), fill_value=0)
    variable_coordinates(target, var)
    var.long_name = "time that the data was passed"
    var.standard_name = "time"
    var.units = "milliseconds since 1970-01-01 00:00:00"
    return var


def pass_profile(target: Dataset, profiles: typing.Union[typing.List[str], typing.Dict[str, int]]) -> Variable:
    if isinstance(profiles, dict):
        max_number = max(profiles.values())
        profile_enum_dict = profiles
    else:
        max_number = len(profiles)
        profile_enum_dict = {
            profiles[index]: index for index in range(len(profiles))
        }

    dtype = None
    for check_type in (np.uint8, np.uint16, np.uint32, np.uint64):
        ti = np.iinfo(check_type)
        if ti.max < max_number:
            continue
        dtype = check_type
        break
    else:
        raise ValueError("Invalid profile count")

    profile_t = target.createEnumType(dtype, "profile_t", profile_enum_dict)
    var = target.createVariable("profile", profile_t, ("index",), fill_value=False)
    variable_coordinates(target, var)
    var.long_name = "the profile of data passed"
    return var


def pass_comment(target: Dataset) -> Variable:
    var = target.createVariable("comment", str, ("index",), fill_value=False)
    variable_coordinates(target, var)
    var.long_name = "pass comment"
    var.text_encoding = "UTF-8"
    return var


def pass_auxiliary(target: Dataset) -> Variable:
    var = target.createVariable("auxiliary_data", str, ("index",), fill_value=False)
    variable_coordinates(target, var)
    var.long_name = "JSON encoded auxiliary data"
    var.text_encoding = "UTF-8"
    return var


def passed_structure(root: Dataset, profiles: typing.Union[typing.List[str], typing.Dict[str, int]]) -> Group:
    passed = root.createGroup("passed")
    pass_bounds(passed)
    pass_time(passed)
    pass_profile(passed, profiles)
    pass_comment(passed)
    pass_auxiliary(passed)
    return passed
