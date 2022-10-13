import typing


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
