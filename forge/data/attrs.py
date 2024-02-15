import typing
from netCDF4 import Variable, Dataset


def cell_methods(var: Variable) -> typing.Dict[str, str]:
    try:
        methods = var.cell_methods
    except AttributeError:
        return dict()

    components = methods.strip().split(':')
    result: typing.Dict[str, str] = dict()
    for idx in range(1, len(components)):
        var_name = components[idx-1].split()[-1]
        if idx == len(components)-1:
            var_methods = components[idx].strip()
        else:
            var_methods = " ".join(components[idx].split()[:-1])
        result[var_name] = var_methods
    return result


def copy(source: typing.Union[Variable, Dataset], destination: typing.Union[Variable, Dataset]) -> None:
    for attr in source.ncattrs():
        if attr.startswith('_'):
            continue
        destination.setncattr(attr, source.getncattr(attr))
