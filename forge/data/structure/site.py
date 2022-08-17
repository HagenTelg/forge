import typing
from netCDF4 import Dataset
from forge.processing.station.lookup import station_data
from . import apply_attribute


def station_code(root: Dataset, value: str, change_value: bool = True) -> None:
    var = root.variables.get("station_name")
    if var is not None and not change_value:
        return
    if not var:
        var = root.createVariable("station_name", str, fill_value=False)
        var.long_name = "station code"
        var.cf_role = "timeseries_id"
        var.standard_name = "platform_id"
        var.coverage_content_type = "coordinate"
    var[0] = value


def latitude(root: Dataset, value: float, change_value: bool = True) -> None:
    var = root.variables.get("lat")
    if var is not None and not change_value:
        return
    if not var:
        var = root.createVariable("lat", 'f8', fill_value=False)
        var.axis = "Y"
        var.long_name = "station latitude"
        var.standard_name = "latitude"
        var.coverage_content_type = "coordinate"
        var.units = "degrees_north"
        var.C_format = "%.4f"
    var[0] = value


def longitude(root: Dataset, value: float, change_value: bool = True) -> None:
    var = root.variables.get("lon")
    if var is not None and not change_value:
        return
    if not var:
        var = root.createVariable("lon", 'f8', fill_value=False)
        var.axis = "X"
        var.long_name = "station longitude"
        var.standard_name = "longitude"
        var.coverage_content_type = "coordinate"
        var.units = "degrees_east"
        var.C_format = "%.4f"
    var[0] = value


def altitude(root: Dataset, value: float, change_value: bool = True) -> None:
    var = root.variables.get("alt")
    if var is not None and not change_value:
        return
    if not var:
        var = root.createVariable("alt", 'f8', fill_value=False)
        var.axis = "Z"
        var.long_name = "height above mean sea level"
        var.positive = "up"
        var.standard_name = "altitude"
        var.coverage_content_type = "coordinate"
        var.units = "m"
        var.C_format = "%.0f"
    var[0] = value


def inlet_height(root: Dataset, value: float, change_value: bool = True) -> None:
    var = root.variables.get("station_inlet_height")
    if var is not None and not change_value:
        return
    if not var:
        var = root.createVariable("station_inlet_height", 'f8', fill_value=False)
        var.axis = "Z"
        var.long_name = "inlet height above the surface"
        var.positive = "up"
        var.standard_name = "height"
        var.coverage_content_type = "coordinate"
        var.units = "m"
        var.C_format = "%.0f"
    var[0] = value


def name(root: Dataset, value: str, change_value: bool = True) -> None:
    var = root.variables.get("station_name")
    if var is None:
        return
    apply_attribute(var, "descriptive_name", value, change_value=change_value)


def country_code(root: Dataset, value: str, change_value: bool = True) -> None:
    var = root.variables.get("station_name")
    if var is None:
        return
    apply_attribute(var, "country_code", value, change_value=change_value)


def subdivision(root: Dataset, value: str, change_value: bool = True) -> None:
    var = root.variables.get("station_name")
    if var is None:
        return
    apply_attribute(var, "country_subdivision", value, change_value=change_value)


def set_site(root: Dataset, station: str, tags: typing.Optional[typing.Set[str]] = None,
             override: typing.Optional[typing.Callable[[str], typing.Any]] = None) -> None:
    station_code(root, station.upper())

    for code in ('latitude', 'longitude', 'altitude', 'inlet_height',
                 'name', 'country_code', 'subdivision'):
        if override:
            value = override(code)
        else:
            value = None
        if value is None:
            value = station_data(station, 'site', code)(station, tags)
        if value is None:
            continue
        globals()[code](root, value, change_value=False)
