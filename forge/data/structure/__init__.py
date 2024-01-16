import typing
import netCDF4


def apply_attribute(target: typing.Union[netCDF4.Dataset, netCDF4.Variable],
                    name: str, value: typing.Any, change_value: bool = True) -> None:
    if not change_value:
        if name in target.ncattrs():
            return
    target.setncattr(name, value)


def find_variable(origin: netCDF4.Dataset, name: str) -> typing.Optional[netCDF4.Variable]:
    check = origin.variables.get(name, None)
    if check is not None:
        return check
    if origin.parent is None:
        return None
    return find_variable(origin.parent, name)


def instrument_timeseries(root: netCDF4.Dataset, station: str, instrument: str, start_epoch: float, end_epoch: float,
                          interval: typing.Optional[float] = None,
                          tags: typing.Optional[typing.Set[str]] = None,
                          override: typing.Optional[typing.Callable[[str], typing.Any]] = None) -> None:
    from .basic import set_basic
    from .dataset import set_dataset
    from .site import set_site
    from .ebas import set_ebas
    from .timeseries import set_timeseries

    set_basic(root)
    set_dataset(root, station, tags=tags, override=override)
    set_site(root, station, tags=tags, override=override)
    set_ebas(root, station, tags=tags, override=override)
    set_timeseries(root, f"{station.upper()}-{instrument}", start_epoch, end_epoch, interval)
    apply_attribute(root, "instrument_id", instrument)
    if tags:
        sorted_tags = list(tags)
        sorted_tags.sort()
        apply_attribute(root, "forge_tags", " ".join(sorted_tags))


def event_log(root: netCDF4.Dataset, station: str, start_epoch: float, end_epoch: float,
              override: typing.Optional[typing.Callable[[str], typing.Any]] = None) -> None:
    from .basic import set_basic
    from .dataset import set_dataset
    from .site import set_site
    from .ebas import set_ebas
    from .timeseries import set_timeseries

    tags = {"eventlog"}

    set_basic(root)
    set_dataset(root, station, tags=tags, override=override)
    set_site(root, station, tags=tags, override=override)
    set_ebas(root, station, tags=tags, override=override)
    set_timeseries(root, f"{station.upper()}-LOG", start_epoch, end_epoch)
    apply_attribute(root, "forge_tags", " ".join(tags))


def edit_directives(root: netCDF4.Dataset, station: str,
                    start_epoch: typing.Optional[float], end_epoch: typing.Optional[float],
                    override: typing.Optional[typing.Callable[[str], typing.Any]] = None) -> None:
    from .basic import set_basic
    from .dataset import set_dataset
    from .site import set_site
    from .ebas import set_ebas
    from .timeseries import set_timeseries

    tags = {"edits"}

    set_basic(root)
    set_dataset(root, station, tags=tags, override=override)
    set_site(root, station, tags=tags, override=override)
    set_ebas(root, station, tags=tags, override=override)
    set_timeseries(root, f"{station.upper()}-EDITS", start_epoch, end_epoch)
    apply_attribute(root, "forge_tags", " ".join(tags))
