import typing
import asyncio
from ..cpd3 import Export, NativeExport, ExportList, DataExportList, export_profile_get, export_profile_lookup, detach, profile_export

station_profile_export = detach(profile_export)


station_profile_export['aerosol']['raw'].insert(
    DataExportList.Entry('cpd3native', "CPD3 Native Format", lambda station, start_epoch_ms, end_epoch_ms, directory: NativeExport(
        start_epoch_ms, end_epoch_ms, directory, station, 'raw',
    )),
)
station_profile_export['aerosol']['clean'].insert(
    DataExportList.Entry('cpd3native', "CPD3 Native Format", lambda station, start_epoch_ms, end_epoch_ms, directory: NativeExport(
        start_epoch_ms, end_epoch_ms, directory, station, 'clean',
    )),
)
station_profile_export['aerosol']['avgh'].insert(
    DataExportList.Entry('cpd3native', "CPD3 Native Format", lambda station, start_epoch_ms, end_epoch_ms, directory: NativeExport(
        start_epoch_ms, end_epoch_ms, directory, station, 'avgh',
    )),
)


def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
    return export_profile_get(station, mode_name, export_key,
                              start_epoch_ms, end_epoch_ms, directory, station_profile_export)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_profile_lookup(station, mode_name, station_profile_export)
