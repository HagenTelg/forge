import typing
import netCDF4
from forge.cpd3.identity import Identity
from ..default.converter import Converter as BaseConverter


class Converter(BaseConverter):
    def __init__(self, station: str, root: netCDF4.Dataset):
        super().__init__(station, root)

        inst_group = self.root.groups.get("instrument")
        if inst_group is not None:
            calibration = inst_group.variables.get("calibration")
            if calibration is not None:
                self.source_metadata["CalibrationDate"] = str(calibration[0])


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    return Converter(station, root).convert()
