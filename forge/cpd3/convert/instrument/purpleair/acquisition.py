import typing
import netCDF4
from forge.cpd3.identity import Identity
from ..default.converter import Converter as BaseConverter


class Converter(BaseConverter):
    def __init__(self, station: str, root: netCDF4.Dataset):
        super().__init__(station, root)

        inst_group = self.root.groups.get("instrument")
        if inst_group is not None:
            mac_address = inst_group.variables.get("mac_address")
            if mac_address is not None:
                self.source_metadata["MACAddress"] = str(mac_address[0])
            hardware = inst_group.variables.get("hardware")
            if hardware is not None:
                self.source_metadata["SensorHardware"] = str(hardware[0])


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    return Converter(station, root).convert()
