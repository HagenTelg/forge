import typing
from forge.vis.export import Export, ExportList


def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
    from forge.vis.station.cpd3 import export_get
    return export_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    from forge.vis.station.cpd3 import export_available
    return export_available(station, mode_name)
