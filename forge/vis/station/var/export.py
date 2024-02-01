import typing
from ..cpd3 import Export, ExportList, DataExportList, DataExport, Name, export_profile_get, export_profile_lookup, detach, profile_export


station_profile_export = detach(profile_export)


station_profile_export['aerosol']['raw'].insert(
    DataExportList.Entry('nephzero', "Nephelometer Zero", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
            Name(station, 'raw', 'Tw_S11'),
            Name(station, 'raw', 'Pw_S11'),
            Name(station, 'raw', 'BswB_S11'),
            Name(station, 'raw', 'BswG_S11'),
            Name(station, 'raw', 'BswR_S11'),
            Name(station, 'raw', 'BbswB_S11'),
            Name(station, 'raw', 'BbswG_S11'),
            Name(station, 'raw', 'BbswR_S11'),
        },
    ), time_limit_days=None),
)


def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
    return export_profile_get(station, mode_name, export_key,
                              start_epoch_ms, end_epoch_ms, directory, station_profile_export)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_profile_lookup(station, mode_name, station_profile_export)
