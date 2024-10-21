import typing
import numpy as np
from netCDF4 import Variable


def remap_enum(input_variable: Variable, output_variable: Variable,
               remove_index: typing.Optional[int] = None) -> None:
    input_values = input_variable[...].data
    output_map = output_variable.datatype.enum_dict
    assign_values = np.empty(input_variable.shape, dtype=output_variable.dtype)
    for map_code, map_value in input_variable.datatype.enum_dict.items():
        assign_values[input_values == map_value] = output_map[map_code]
    if remove_index is None:
        output_variable[...] = assign_values
    else:
        if remove_index != 0:
            output_variable[:remove_index] = assign_values[:remove_index]
        if remove_index != input_values.shape[0] - 1:
            output_variable[remove_index:] = assign_values[remove_index + 1:]
