import typing
from netCDF4 import Dataset, Variable, EnumType
from .timeseries import time_coordinate, variable_coordinates


def event_time(target: Dataset) -> Variable:
    var = time_coordinate(target)
    var.long_name = "time of event"
    return var


def event_type(target: Dataset, event_t: EnumType) -> Variable:
    var = target.createVariable("type", event_t, ("time",), fill_value=False)
    variable_coordinates(target, var)
    var.long_name = "event type"
    var.cell_methods = "time: point"
    return var


def event_source(target: Dataset) -> Variable:
    var = target.createVariable("source", str, ("time",), fill_value=False)
    variable_coordinates(target, var)
    var.long_name = "source name"
    var.text_encoding = "UTF-8"
    var.cell_methods = "time: point"
    return var


def event_message(target: Dataset) -> Variable:
    var = target.createVariable("message", str, ("time",), fill_value=False)
    variable_coordinates(target, var)
    var.long_name = "text description of the event"
    var.text_encoding = "UTF-8"
    var.cell_methods = "time: point"
    return var


def event_auxiliary(target: Dataset) -> Variable:
    var = target.createVariable("auxiliary_data", str, ("time",), fill_value=False)
    variable_coordinates(target, var)
    var.long_name = "JSON encoded auxiliary data"
    var.text_encoding = "UTF-8"
    var.cell_methods = "time: point"
    return var

