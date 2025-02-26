import typing
import asyncio
from ..cpd3 import Name, Export, DataExport, NativeExport, ExportList, DataExportList, export_profile_get, export_profile_lookup, detach, profile_export

station_profile_export = detach(profile_export)


station_profile_export['aerosol']['raw'].insert(
    DataExportList.Entry('ambient', "Ambient Meteorological", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
            Name(station, 'raw', 'WS_XM1'), Name(station, 'raw', 'WD_XM1'),
            Name(station, 'raw', 'WS_XM3'), Name(station, 'raw', 'WD_XM3'),
            Name(station, 'raw', 'WI_XM1'), Name(station, 'raw', 'WI_XM2'),
            Name(station, 'raw', 'WX1_XM2'), Name(station, 'raw', 'WX2_XM2'),
            Name(station, 'raw', 'T1_XM1'), Name(station, 'raw', 'U1_XM1'), Name(station, 'raw', 'TD1_XM1'),
            Name(station, 'raw', 'T1_XM2'),
            Name(station, 'raw', 'P_XM1'),
            Name(station, 'raw', 'VA_XM1'),
            Name(station, 'raw', 'WZ_XM2'),
            Name(station, 'raw', 'ZWSGust_XM1'),
        },
    ))
)


def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
    return export_profile_get(station, mode_name, export_key,
                              start_epoch_ms, end_epoch_ms, directory, station_profile_export)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_profile_lookup(station, mode_name, station_profile_export)
