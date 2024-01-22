import typing
import numpy as np
from netCDF4 import Dataset, Variable, Group
from forge.const import MAX_I64
from .timeseries import variable_coordinates


def edit_bounds(target: Dataset) -> typing.Tuple[Variable, Variable]:
    dim = target.createDimension("index", None)

    start = target.createVariable("start_time", "i8", ("index",), fill_value=-MAX_I64)
    variable_coordinates(target, start)
    start.long_name = "inclusive start time of the effect of the edit"
    start.standard_name = "time"
    start.coverage_content_type = "coordinate"
    start.units = "milliseconds since 1970-01-01 00:00:00"

    end = target.createVariable("end_time", "i8", ("index",), fill_value=MAX_I64)
    variable_coordinates(target, end)
    end.long_name = "exclusive end time of the effect of the edit"
    end.standard_name = "time"
    end.coverage_content_type = "coordinate"
    end.units = "milliseconds since 1970-01-01 00:00:00"

    return start, end


def edit_modified(target: Dataset) -> Variable:
    var = target.createVariable("modified_time", "i8", ("index",), fill_value=0)
    variable_coordinates(target, var)
    var.long_name = "modification time"
    var.standard_name = "time"
    var.units = "milliseconds since 1970-01-01 00:00:00"
    return var


def edit_unique_id(target: Dataset) -> Variable:
    var = target.createVariable("unique_id", "u8", ("index",), fill_value=0)
    variable_coordinates(target, var)
    var.long_name = "unique identifier of the edit across files"
    var.C_format = "%016llX"
    return var


def edit_deleted(target: Dataset) -> Variable:
    deleted_t = target.createEnumType("u1", "deleted_t", {
        "Active": 0,
        "Deleted": 1,
    })

    var = target.createVariable("deleted", deleted_t, ("index",), fill_value=False)
    variable_coordinates(target, var)
    var.long_name = "indicator if the edit has been deleted and does not affect data"
    return var


def edit_profile(target: Dataset, profiles: typing.Union[typing.List[str], typing.Dict[str, int]]) -> Variable:
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
    var.long_name = "the profile the edit is associated with"
    return var


def edit_action_type(target: Dataset) -> Variable:
    action_t = target.createEnumType("u1", "action_t", {
        "Invalidate": 0,
        "Contaminate": 1,
        "Calibration": 2,
        "Recalibrate": 3,
        "FlowCorrection": 4,
        "SizeCutFix": 5,
        "AbnormalData": 6,
    })

    var = target.createVariable("action_type", action_t, ("index",), fill_value=False)
    variable_coordinates(target, var)
    var.long_name = "edit action type"
    return var


def edit_action_parameters(target: Dataset) -> Variable:
    var = target.createVariable("action_parameters", str, ("index",), fill_value=False)
    variable_coordinates(target, var)
    var.long_name = "JSON encoded action parameters"
    var.text_encoding = "UTF-8"
    return var


def edit_condition_type(target: Dataset) -> Variable:
    condition_t = target.createEnumType("u1", "condition_t", {
        "None": 0,
        "Threshold": 1,
        "Periodic": 2,
    })

    var = target.createVariable("condition_type", condition_t, ("index",), fill_value=False)
    variable_coordinates(target, var)
    var.long_name = "edit conditional activation type"
    return var


def edit_condition_parameters(target: Dataset) -> Variable:
    var = target.createVariable("condition_parameters", str, ("index",), fill_value=False)
    variable_coordinates(target, var)
    var.long_name = "JSON encoded condition activation parameters"
    var.text_encoding = "UTF-8"
    return var


def edit_author(target: Dataset) -> Variable:
    var = target.createVariable("author", str, ("index",), fill_value=False)
    variable_coordinates(target, var)
    var.long_name = "edit author"
    var.text_encoding = "UTF-8"
    return var


def edit_comment(target: Dataset) -> Variable:
    var = target.createVariable("comment", str, ("index",), fill_value=False)
    variable_coordinates(target, var)
    var.long_name = "edit comment"
    var.text_encoding = "UTF-8"
    return var


def edit_history(target: Dataset) -> Variable:
    var = target.createVariable("history", str, ("index",), fill_value=False)
    variable_coordinates(target, var)
    var.long_name = "JSON encoded history information"
    var.text_encoding = "UTF-8"
    return var


def edit_file_structure(root: Dataset, profiles: typing.Union[typing.List[str], typing.Dict[str, int]]) -> Group:
    edits = root.createGroup("edits")
    edit_bounds(edits)
    edit_modified(edits)
    edit_unique_id(edits)
    edit_deleted(edits)
    edit_profile(edits, profiles)
    edit_action_type(edits)
    edit_action_parameters(edits)
    edit_condition_type(edits)
    edit_condition_parameters(edits)
    edit_author(edits)
    edit_comment(edits)
    edit_history(edits)
    return edits
