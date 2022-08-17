import typing
from netCDF4 import Dataset
from forge.processing.station.lookup import station_data
from . import apply_attribute


def identifier(root: Dataset, value: str, change_value: bool = True) -> None:
    var = root.variables.get("station_name")
    if var is None:
        return
    apply_attribute(var, "ebas_identifier", value, change_value=change_value)


def set_ebas(root: Dataset, station: str, tags: typing.Optional[typing.Set[str]] = None,
             override: typing.Optional[typing.Callable[[str], typing.Any]] = None) -> None:
    if override:
        value = override('ebas_identifier')
    else:
        value = None
    if value is None:
        value = ""

        add = station_data(station, 'ebas', 'station')(station, tags)
        if add:
            if value:
                value = value + " "
            value = value + add

        add = station_data(station, 'ebas', 'platform')(station, tags)
        if add:
            if value:
                value = value + " "
            value = value + add

    if value:
        identifier(root, value, change_value=False)
