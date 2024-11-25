import typing
import re


class UnitData:
    def __init__(self, units: typing.Optional[str] = None, format: typing.Optional[str] = None):
        self.units = units
        self.format = format


units: typing.Dict[str, UnitData] = {
    "degC": UnitData("°C", "00.0"),
    "cm-3": UnitData("cm⁻³", "00000.0"),
    "Mm-1": UnitData("Mm⁻¹", "0000.00"),
    "m-1": UnitData("m⁻¹", "0.000E0"),
    "ug m-3": UnitData("μg/m³", "0000.000"),
    "1e-9": UnitData("ppb", "000000.00"),
    "m s-1": UnitData("m/s", "00.0"),
    "degree": UnitData("degrees", "000.0"),
    "%": UnitData(format="00.0"),
    "hPa": UnitData(format="0000.0"),
    "lpm": UnitData(format="00.000"),
}


_CPD3_DECIMAL = re.compile(r"(\d+)(?:\.(\d+))?")
_CPD3_SCIENTIFIC = re.compile(r"(\d+)(\.(\d+))?E\d+?")


def from_cpd3(cpd3_units: typing.Optional[str], cpd3_format: typing.Optional[str] = None) -> UnitData:
    selected_nc_unit: typing.Optional[str] = None
    effective_format = cpd3_format
    for nc_unit, cpd3_data in units.items():
        if cpd3_data.units != cpd3_units:
            continue
        selected_nc_unit = nc_unit
        if not effective_format:
            effective_format = cpd3_data.format
        break

    nc_format: typing.Optional[str] = None
    if effective_format:
        match = _CPD3_DECIMAL.fullmatch(effective_format)
        if match:
            integers = len(match.group(1))
            decimals = match.group(2)
            if not decimals:
                nc_format = f"%{integers}.0f"
            else:
                decimals = int(len(decimals))
                total_width = integers + 1 + decimals
                nc_format = f"%{total_width}.{decimals}f"
        else:
            match = _CPD3_SCIENTIFIC.fullmatch(effective_format)
            if match:
                decimals = match.group(2)
                if not decimals:
                    nc_format = "%.0e"
                else:
                    decimals = int(len(decimals))
                    nc_format = f"%.{decimals}e"

    return UnitData(selected_nc_unit, nc_format)
