import typing
import numpy as np
from netCDF4 import Dataset, Variable, VLType, EnumType
from .attrs import copy as copy_attrs


def copy_variable_values(source: Variable, destination: Variable) -> None:
    if isinstance(destination.datatype, VLType):
        for idx in np.ndindex(source.shape):
            destination[idx] = source[idx]
    else:
        destination[...] = source[...].data


def append_variable_values(source: Variable, destination: Variable, begin_index: int) -> None:
    if isinstance(destination.datatype, VLType):
        for idx in np.ndindex(source.shape):
            destination[(idx[0] + begin_index, *idx[1:])] = source[idx]
    else:
        destination[begin_index:] = source[...].data


def create_and_copy_variable(source: Variable, destination: Dataset, copy_values: bool = True,
                             dimensions: typing.List[str] = None) -> Variable:
    if isinstance(source.datatype, EnumType):
        dtype = destination.enumtypes.get(source.datatype.name)
        if not dtype:
            dtype = destination.createEnumType(source.datatype.dtype, source.datatype.name,
                                               source.datatype.enum_dict)
    else:
        dtype = source.dtype

    fill_value = False
    try:
        fill_value = source._FillValue
    except AttributeError:
        pass

    output_variable = destination.createVariable(
        source.name, dtype,
        source.dimensions if dimensions is None else dimensions,
        fill_value=fill_value
    )
    copy_attrs(source, output_variable)
    if copy_values:
        assert dimensions is None
        copy_variable_values(source, output_variable)

    return output_variable
