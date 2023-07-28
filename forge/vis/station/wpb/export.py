import typing
from ..cpd3 import Export, ExportList, DataExportList, DataExport, NativeExport, Name, export_profile_get, export_profile_lookup, detach, profile_export


station_profile_export = detach(profile_export)


station_profile_export['aerosol']['raw']['absorption'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
    start_epoch_ms, end_epoch_ms, directory, 'basic', {
        Name(station, 'raw', 'Q_A11'),
        Name(station, 'raw', 'L_A11'),
        Name(station, 'raw', 'Fn_A11'),
        Name(station, 'raw', 'BaB_A11'),
        Name(station, 'raw', 'BaG_A11'),
        Name(station, 'raw', 'BaR_A11'),
        Name(station, 'raw', 'IrB_A11'),
        Name(station, 'raw', 'IrG_A11'),
        Name(station, 'raw', 'IrR_A11'),
        Name(station, 'raw', 'Q_A12'),
        Name(station, 'raw', 'L_A12'),
        Name(station, 'raw', 'Fn_A12'),
        Name(station, 'raw', 'BaB_A12'),
        Name(station, 'raw', 'BaG_A12'),
        Name(station, 'raw', 'BaR_A12'),
        Name(station, 'raw', 'IrB_A12'),
        Name(station, 'raw', 'IrG_A12'),
        Name(station, 'raw', 'IrR_A12'),
    },
)
station_profile_export['aerosol']['clean']['absorption'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
    start_epoch_ms, end_epoch_ms, directory, 'basic', {
        Name(station, 'clean', 'Q_A11'),
        Name(station, 'clean', 'L_A11'),
        Name(station, 'clean', 'Fn_A11'),
        Name(station, 'clean', 'BaB_A11'),
        Name(station, 'clean', 'BaG_A11'),
        Name(station, 'clean', 'BaR_A11'),
        Name(station, 'clean', 'IrB_A11'),
        Name(station, 'clean', 'IrG_A11'),
        Name(station, 'clean', 'IrR_A11'),
        Name(station, 'clean', 'Q_A12'),
        Name(station, 'clean', 'L_A12'),
        Name(station, 'clean', 'Fn_A12'),
        Name(station, 'clean', 'BaB_A12'),
        Name(station, 'clean', 'BaG_A12'),
        Name(station, 'clean', 'BaR_A12'),
        Name(station, 'clean', 'IrB_A12'),
        Name(station, 'clean', 'IrG_A12'),
        Name(station, 'clean', 'IrR_A12'),
    },
)
station_profile_export['aerosol']['avgh']['absorption'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
    start_epoch_ms, end_epoch_ms, directory, 'basic', {
        Name(station, 'avgh', 'Q_A11'),
        Name(station, 'avgh', 'L_A11'),
        Name(station, 'avgh', 'Fn_A11'),
        Name(station, 'avgh', 'BaB_A11'),
        Name(station, 'avgh', 'BaG_A11'),
        Name(station, 'avgh', 'BaR_A11'),
        Name(station, 'avgh', 'IrB_A11'),
        Name(station, 'avgh', 'IrG_A11'),
        Name(station, 'avgh', 'IrR_A11'),
        Name(station, 'avgh', 'Q_A12'),
        Name(station, 'avgh', 'L_A12'),
        Name(station, 'avgh', 'Fn_A12'),
        Name(station, 'avgh', 'BaB_A12'),
        Name(station, 'avgh', 'BaG_A12'),
        Name(station, 'avgh', 'BaR_A12'),
        Name(station, 'avgh', 'IrB_A12'),
        Name(station, 'avgh', 'IrG_A12'),
        Name(station, 'avgh', 'IrR_A12'),
    },
)


def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
    return export_profile_get(station, mode_name, export_key,
                              start_epoch_ms, end_epoch_ms, directory, station_profile_export)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_profile_lookup(station, mode_name, station_profile_export)
