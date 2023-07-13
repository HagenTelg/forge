import typing
from ..cpd3 import Export, ExportList, DataExportList, DataExport, NativeExport, Name, export_profile_get, export_profile_lookup, detach, profile_export


station_profile_export = detach(profile_export)


station_profile_export['aerosol']['raw']['scattering'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
    start_epoch_ms, end_epoch_ms, directory, 'basic', {
        Name(station, 'raw', 'T_S11'),
        Name(station, 'raw', 'P_S11'),
        Name(station, 'raw', 'U_S11'),
        Name(station, 'raw', 'T_S12'),
        Name(station, 'raw', 'P_S12'),
        Name(station, 'raw', 'U_S12'),
        Name(station, 'raw', 'BsB_S11'),
        Name(station, 'raw', 'BsG_S11'),
        Name(station, 'raw', 'BsR_S11'),
        Name(station, 'raw', 'BbsB_S11'),
        Name(station, 'raw', 'BbsG_S11'),
        Name(station, 'raw', 'BbsR_S11'),
        Name(station, 'raw', 'BsB_S12'),
        Name(station, 'raw', 'BsG_S12'),
        Name(station, 'raw', 'BsR_S12'),
        Name(station, 'raw', 'BbsB_S12'),
        Name(station, 'raw', 'BbsG_S12'),
        Name(station, 'raw', 'BbsR_S12'),
    },
)
station_profile_export['aerosol']['clean']['scattering'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
    start_epoch_ms, end_epoch_ms, directory, 'basic', {
        Name(station, 'clean', 'T_S11'),
        Name(station, 'clean', 'P_S11'),
        Name(station, 'clean', 'U_S11'),
        Name(station, 'clean', 'T_S12'),
        Name(station, 'clean', 'P_S12'),
        Name(station, 'clean', 'U_S12'),
        Name(station, 'clean', 'BsB_S11'),
        Name(station, 'clean', 'BsG_S11'),
        Name(station, 'clean', 'BsR_S11'),
        Name(station, 'clean', 'BbsB_S11'),
        Name(station, 'clean', 'BbsG_S11'),
        Name(station, 'clean', 'BbsR_S11'),
        Name(station, 'clean', 'BsB_S12'),
        Name(station, 'clean', 'BsG_S12'),
        Name(station, 'clean', 'BsR_S12'),
        Name(station, 'clean', 'BbsB_S12'),
        Name(station, 'clean', 'BbsG_S12'),
        Name(station, 'clean', 'BbsR_S12'),
    },
)
station_profile_export['aerosol']['avgh']['scattering'].data = lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
    start_epoch_ms, end_epoch_ms, directory, 'basic', {
        Name(station, 'avgh', 'T_S11'),
        Name(station, 'avgh', 'P_S11'),
        Name(station, 'avgh', 'U_S11'),
        Name(station, 'avgh', 'T_S12'),
        Name(station, 'avgh', 'P_S12'),
        Name(station, 'avgh', 'U_S12'),
        Name(station, 'avgh', 'BsB_S11'),
        Name(station, 'avgh', 'BsG_S11'),
        Name(station, 'avgh', 'BsR_S11'),
        Name(station, 'avgh', 'BbsB_S11'),
        Name(station, 'avgh', 'BbsG_S11'),
        Name(station, 'avgh', 'BbsR_S11'),
        Name(station, 'avgh', 'BsB_S12'),
        Name(station, 'avgh', 'BsG_S12'),
        Name(station, 'avgh', 'BsR_S12'),
        Name(station, 'avgh', 'BbsB_S12'),
        Name(station, 'avgh', 'BbsG_S12'),
        Name(station, 'avgh', 'BbsR_S12'),
    },
)


def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
    return export_profile_get(station, mode_name, export_key,
                              start_epoch_ms, end_epoch_ms, directory, station_profile_export)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_profile_lookup(station, mode_name, station_profile_export)
