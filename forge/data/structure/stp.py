import typing
from netCDF4 import Dataset
from forge.units import ONE_ATM_IN_HPA


def standard_pressure(target: Dataset, value: float = ONE_ATM_IN_HPA, change_value: bool = True) -> None:
    var = target.variables.get("standard_pressure")
    if var is not None and not change_value:
        return
    if var is None:
        var = target.createVariable("standard_pressure", 'f8', fill_value=False)
        var.long_name = "standard pressure"
        var.coverage_content_type = "auxiliaryInformation"
        var.units = "hPa"
        var.C_format = "%6.1f"
    var[0] = value


def standard_temperature(target: Dataset, value: float = 0.0, change_value: bool = True) -> None:
    var = target.variables.get("standard_temperature")
    if var is not None and not change_value:
        return
    if var is None:
        var = target.createVariable("standard_temperature", 'f8', fill_value=False)
        var.long_name = "standard temperature"
        var.coverage_content_type = "auxiliaryInformation"
        var.units = "degC"
        var.C_format = "%4.1f"
    var[0] = value
