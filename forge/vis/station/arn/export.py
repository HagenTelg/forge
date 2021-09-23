import typing
from ..cpd3 import Export, ExportList, DataExportList, DataExport, Name, export_profile_get, export_profile_lookup, detach, profile_export


station_profile_export = detach(profile_export)

station_profile_export['aerosol']['raw']['counts'].data = lambda station, start_epoch_ms, end_epoch_ms: DataExport(
    start_epoch_ms, end_epoch_ms, 'unsplit', {
        Name(station, 'raw', 'N_N23'),
    },
)
station_profile_export['aerosol']['clean']['counts'].data = lambda station, start_epoch_ms, end_epoch_ms: DataExport(
    start_epoch_ms, end_epoch_ms, 'unsplit', {
        Name(station, 'clean', 'N_N23'),
    },
)
station_profile_export['aerosol']['avgh']['counts'].data = lambda station, start_epoch_ms, end_epoch_ms: DataExport(
    start_epoch_ms, end_epoch_ms, 'average', {
        Name(station, 'avgh', 'N_N23'),
    },
)

def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int) -> typing.Optional[Export]:
    return export_profile_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, station_profile_export)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_profile_lookup(station, mode_name, station_profile_export)
