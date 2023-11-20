import typing
import time
from netCDF4 import Dataset, Variable
from numpy import dtype
from math import nan
from forge.formattime import format_iso8601_time, format_iso8601_duration
from . import find_variable
from .basic import date_created
from .variable import variable_cutsize as setup_cutsize


def time_coverage_start(root: Dataset, start_epoch: float) -> None:
    root.setncattr("time_coverage_start", format_iso8601_time(start_epoch))


def time_coverage_end(root: Dataset, end_epoch: float) -> None:
    root.setncattr("time_coverage_end", format_iso8601_time(end_epoch))


def time_coverage_resolution(root: Dataset, interval: typing.Optional[float]) -> None:
    if not interval or interval < 1.0:
        if "time_coverage_resolution" in root.ncattrs():
            root.delncattr("time_coverage_resolution", None)
        return

    root.setncattr("time_coverage_resolution", format_iso8601_duration(interval))


def _format_time_id(ts: float) -> str:
    return format_iso8601_time(ts, delimited=False)


def file_id(root: Dataset, code: str,
            start_epoch: typing.Optional[float] = None,
            end_epoch: typing.Optional[float] = None,
            now: typing.Optional[float] = None) -> None:
    if not now:
        now = time.time()
    if start_epoch and end_epoch:
        root.setncattr("id", f"{code}_s{_format_time_id(start_epoch)}_e{_format_time_id(end_epoch)}_c{_format_time_id(now)}")
    elif start_epoch:
        root.setncattr("id", f"{code}_s{_format_time_id(start_epoch)}_c{_format_time_id(now)}")
    else:
        root.setncattr("id", f"{code}_c{_format_time_id(now)}")


def set_timeseries(root: Dataset, file_code: str,
                   start_epoch: typing.Optional[float] = None,
                   end_epoch: typing.Optional[float] = None,
                   interval: typing.Optional[float] = None) -> None:
    now = time.time()
    date_created(root, now)
    file_id(root, file_code, start_epoch, end_epoch, now=now)
    if start_epoch:
        time_coverage_start(root, start_epoch)
    if end_epoch:
        time_coverage_end(root, end_epoch)
    time_coverage_resolution(root, interval)


def time_coordinate(target: Dataset) -> Variable:
    dim = target.createDimension("time", None)
    var = target.createVariable("time", "i8", ("time",), fill_value=False)
    var.long_name = "start time of measurement"
    var.standard_name = "time"
    var.coverage_content_type = "coordinate"
    var.units = "milliseconds since 1970-01-01 00:00:00"
    return var


def state_change_coordinate(target: Dataset) -> Variable:
    dim = target.createDimension("time", None)
    var = target.createVariable("time", "i8", ("time",), fill_value=False)
    var.long_name = "time of change"
    var.standard_name = "time"
    var.coverage_content_type = "coordinate"
    var.units = "milliseconds since 1970-01-01 00:00:00"
    return var


def averaged_time_variable(target: Dataset) -> Variable:
    var = target.createVariable("averaged_time", "u8", ("time",), fill_value=False)
    var.long_name = "total time represented by the average"
    var.coverage_content_type = "referenceInformation"
    var.units = "milliseconds"
    return var


def averaged_count_variable(target: Dataset) -> Variable:
    var = target.createVariable("averaged_count", "u4", ("time",), fill_value=False)
    var.long_name = "total number of reports represented by the average"
    var.coverage_content_type = "referenceInformation"
    return var


def cutsize_variable(target: Dataset) -> Variable:
    var = target.createVariable("cut_size", "f8", ("time",), fill_value=nan)
    setup_cutsize(var)
    var.coverage_content_type = "referenceInformation"  # Not measured, so reference is a bit better fit
    return var


def cutsize_coordinate(target: Dataset, number_possible: int) -> Variable:
    dim = target.createDimension("cut_size", number_possible)
    var = target.createVariable("cut_size", "f8", ("cut_size",), fill_value=nan)
    setup_cutsize(var)
    var.coverage_content_type = "coordinate"
    return var


def variable_coordinates(target: Dataset, var: Variable) -> None:
    coordinates: typing.List[str] = []
    if 'time' in var.dimensions:
        coordinates.append('time')
    if find_variable(target, 'lat') is not None and find_variable(target, 'lon') is not None:
        coordinates.append('lat')
        coordinates.append('lon')
        if find_variable(target, 'alt') is not None:
            coordinates.append('alt')
    if coordinates:
        var.coordinates = " ".join(coordinates)


def _declare_time_variable(target: Dataset, name: str,
                           dimensions: typing.Iterable[str] = None,
                           data_type: typing.Union[str, dtype, typing.Type[str]] = None) -> Variable:
    if not data_type:
        data_type = "f8"
    if dimensions:
        dimensions = tuple((*dimensions, "time"))
    else:
        dimensions = ("time",)
    var = target.createVariable(name, data_type, dimensions, fill_value=False)

    variable_coordinates(target, var)

    return var


def _attribute_kwargs(var: Variable, **kwargs) -> None:
    for key, value in kwargs.items():
        if not key:
            continue
        var.setncattr(key, value)


def measured_variable(target: Dataset,
                      name: str,
                      long_name: typing.Optional[str] = None,
                      units: typing.Optional[str] = None,
                      standard_name: typing.Optional[str] = None,
                      dimensions: typing.Iterable[str] = None,
                      is_stp: bool = False,
                      data_type: typing.Union[str, dtype, typing.Type[str]] = None) -> Variable:
    var = _declare_time_variable(target, name, dimensions, data_type)
    _attribute_kwargs(var, standard_name=standard_name, long_name=long_name, units=units,
                      coverage_content_type="physicalMeasurement")

    if is_stp:
        var.ancillary_variables = "standard_temperature standard_pressure"

    return var


def state_variable(target: Dataset,
                   name: str,
                   long_name: typing.Optional[str] = None,
                   units: typing.Optional[str] = None,
                   standard_name: typing.Optional[str] = None,
                   dimensions: typing.Iterable[str] = None,
                   is_stp: bool = False,
                   data_type: typing.Union[str, dtype, typing.Type[str]] = None) -> Variable:
    var = _declare_time_variable(target, name, dimensions, data_type)
    _attribute_kwargs(var,
                      standard_name=standard_name, long_name=long_name, units=units,
                      coverage_content_type="auxiliaryInformation")

    if is_stp:
        var.ancillary_variables = "standard_temperature standard_pressure"

    return var


def metadata_variable(target: Dataset,
                      name: str,
                      long_name: typing.Optional[str] = None,
                      units: typing.Optional[str] = None,
                      standard_name: typing.Optional[str] = None,
                      time_dependent: bool = False,
                      data_type: typing.Union[str, dtype, typing.Type[str]] = None) -> Variable:
    if not data_type:
        data_type = str
    if time_dependent:
        dimensions = ("time",)
    else:
        dimensions = ()
    var = target.createVariable(name, data_type, dimensions, fill_value=False)

    _attribute_kwargs(var,
                      standard_name=standard_name, long_name=long_name, units=units,
                      coverage_content_type="referenceInformation")

    return var
