import typing
from forge.vis.export import Export, ExportList
from forge.vis.station.cpd3 import Name, DataExport, DataExportList, detach, profile_export, export_profile_get, export_profile_lookup


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

station_profile_export['aerosol']['raw'].insert(
    DataExportList.Entry('met', "Meteorological", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
            Name(station, 'raw', 'WS1_XM1'), Name(station, 'raw', 'WD1_XM1'),
            Name(station, 'raw', 'WS2_XM1'), Name(station, 'raw', 'WD2_XM1'),
            Name(station, 'raw', 'WS3_XM1'), Name(station, 'raw', 'WD3_XM1'),
            Name(station, 'raw', 'T1_XM1'), Name(station, 'raw', 'U1_XM1'), Name(station, 'raw', 'TD1_XM1'),
            Name(station, 'raw', 'T2_XM1'), Name(station, 'raw', 'U2_XM1'), Name(station, 'raw', 'TD2_XM1'),
            Name(station, 'raw', 'T3_XM1'), Name(station, 'raw', 'U3_XM1'), Name(station, 'raw', 'TD3_XM1'),
            Name(station, 'raw', 'P_XM1'),
            Name(station, 'raw', 'WI_XM1'),
        },
    )),
)
station_profile_export['aerosol']['raw'].insert(
    DataExportList.Entry('gas', "Gas Measurements", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'unsplit', {
            Name(station, 'raw', 'X_G81'),
            Name(station, 'raw', 'X_G82'),
            Name(station, 'raw', 'X1_G71'),
            Name(station, 'raw', 'X2_G71'),
        },
    )),
)
station_profile_export['aerosol']['raw'].insert(
    DataExportList.Entry('combined', "Combined Summary", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'basic', {
            Name(station, 'raw', 'BsG_S11'),
            Name(station, 'raw', 'BaG_A11'),
            Name(station, 'raw', 'T1_XM1'),
            Name(station, 'raw', 'U1_XM1'),
            Name(station, 'raw', 'X_G81'),
            Name(station, 'raw', 'X_G82'),
            Name(station, 'raw', 'X1_G71'),
            Name(station, 'raw', 'X2_G71'),
        },
    )),
)
station_profile_export['aerosol']['clean'].insert(
    DataExportList.Entry('combined', "Combined Summary", lambda station, start_epoch_ms, end_epoch_ms, directory: DataExport(
        start_epoch_ms, end_epoch_ms, directory, 'basic', {
            Name(station, 'clean', 'BsG_S11'),
            Name(station, 'clean', 'BaG_A11'),
            Name(station, 'raw', 'T1_XM1'),
            Name(station, 'raw', 'U1_XM1'),
            Name(station, 'raw', 'X_G81'),
            Name(station, 'raw', 'X_G82'),
            Name(station, 'raw', 'X1_G71'),
            Name(station, 'raw', 'X2_G71'),
        },
    )),
)


def get(station: str, mode_name: str, export_key: str,
        start_epoch_ms: int, end_epoch_ms: int, directory: str) -> typing.Optional[Export]:
    return export_profile_get(station, mode_name, export_key, start_epoch_ms, end_epoch_ms, directory, station_profile_export)


async def visible(station: str, mode_name: str) -> typing.Optional[ExportList]:
    return export_profile_lookup(station, mode_name, station_profile_export)
