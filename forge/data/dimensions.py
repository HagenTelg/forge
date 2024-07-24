import typing
from netCDF4 import Dataset, Dimension, Variable


def find_dimension(origin: Dataset, name: str) -> typing.Tuple[Dimension, Variable]:
    while True:
        try:
            return origin.dimensions[name]
        except KeyError:
            origin = origin.parent
            if origin is None:
                raise KeyError(f"Dimension {name} not found")


def find_dimension_values(origin: Dataset, name: str) -> typing.Tuple[Dimension, Variable]:
    while True:
        try:
            dim = origin.dimensions[name]
            var = origin.variables[name]
            if len(var.dimensions) != 1 or var.dimensions[0] != name:
                raise KeyError
            return dim, var
        except KeyError:
            origin = origin.parent
            if origin is None:
                raise KeyError(f"Dimension {name} not found")
