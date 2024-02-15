import typing
from netCDF4 import Variable


def find_statistics_origin(statistics_variable: Variable) -> typing.Optional[Variable]:
    group = statistics_variable.group().parent
    check_path: typing.List[str] = list()
    while group is not None:
        if group.name == 'statistics':
            break
        check_path.append(group.name)
        group = group.parent
    else:
        return None
    group = group.parent
    if group is None:
        return None
    check_path = check_path[:-1]
    for p in check_path:
        group = group.groups.get(p)
        if group is None:
            return None
    return group.variables.get(statistics_variable.name)
