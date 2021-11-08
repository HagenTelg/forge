import typing
from forge.vis.station.lookup import station_data
from . import Export, ExportList


def export_data(station: str, mode_name: str, export_key: str,
                start_epoch_ms: int, end_epoch_ms: int, target_directory: str) -> typing.Optional[Export]:
    if mode_name.startswith("example-"):
        from .example import ExampleExport
        return ExampleExport()

    return station_data(station, 'export', 'get')(station, mode_name, export_key,
                                                  start_epoch_ms, end_epoch_ms, target_directory)


async def visible_exports(station: str, mode_name: str) -> typing.Optional[ExportList]:
    if mode_name.startswith("example-"):
        from .example import ExampleExportList
        return ExampleExportList()

    return await station_data(station, 'export', 'visible')(station, mode_name)
