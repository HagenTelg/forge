import typing
import netCDF4
from forge.cpd3.identity import Identity
from ..default.converter import Converter as BaseConverter, RecordConverter


class Converter(BaseConverter):
    def record_converter(self, group: netCDF4.Group) -> typing.Optional[RecordConverter]:
        if group.name == "parameters":
            return None
        return super().record_converter(group)


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    return Converter(station, root).convert()
