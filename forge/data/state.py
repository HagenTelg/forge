import typing
import re
from netCDF4 import Dataset


_CELL_METHOD_STATE = re.compile(r"(^|\s)time:\s+point(\s|$)")


def is_state_group(group: Dataset) -> typing.Optional[bool]:
    time_variable = group.variables.get('time')
    if time_variable is not None:
        try:
            # Explicit description is conclusive
            if time_variable.long_name == "time of change":
                return True
            if time_variable.long_name == "start time of measurement":
                return False
        except AttributeError:
            pass

    # If all variables with time are point values, assume it's a state measurement, if any have other non-point
    # values, then it can't be state.  If there are no variables with time, then remain unknown.
    result = None
    for var in group.variables.values():
        if len(var.dimensions) == 0:
            continue
        if var.dimensions[0] != 'time':
            continue
        try:
            methods = var.cell_methods
        except AttributeError:
            continue
        if _CELL_METHOD_STATE.search(methods):
            result = True
            continue
        return False
    return result
